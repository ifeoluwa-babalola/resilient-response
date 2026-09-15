import asyncio
import os

from datetime import datetime, timezone, timedelta

import asyncpg


DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:postgres_password@localhost:5432/emergency_db",
)

ACK_TIMEOUT_SECONDS = 15


async def route_incident(
    conn: asyncpg.Connection,
    incident_id: str,
    lng: float,
    lat: float,
    radius_meters: float = 10000.0,
):
    """
    Finds the best eligible responder and starts
    the assignment + ACK timeout lifecycle.
    """

    find_responder_query = """
        SELECT
            responder_id,
            name,
            current_active_incidents,
            ST_Distance(
                location,
                ST_SetSRID(ST_MakePoint($1, $2), 4326)
            ) AS distance_meters
        FROM responders
        WHERE is_eligible = TRUE
          AND is_online = TRUE
          AND ST_DWithin(
                location,
                ST_SetSRID(ST_MakePoint($1, $2), 4326),
                $3
          )
          AND current_active_incidents < active_capacity
        ORDER BY
            current_active_incidents ASC,
            distance_meters ASC
        LIMIT 1
        FOR UPDATE;
    """

    async with conn.transaction():

        #
        # Lock incident first.
        # ACK and Escalator lock incidents first too.
        # Consistent lock ordering prevents deadlocks.
        #
        incident = await conn.fetchrow(
            """
            SELECT incident_id
            FROM incidents
            WHERE incident_id = $1
            FOR UPDATE;
            """,
            incident_id,
        )

        if not incident:
            raise ValueError(
                f"Incident not found: {incident_id}"
            )

        best_responder = await conn.fetchrow(
            find_responder_query,
            lng,
            lat,
            radius_meters,
        )

        #
        # No responder available
        #
        if not best_responder:

            await conn.execute(
                """
                UPDATE incidents
                SET
                    status = 'ESCALATED_TO_SUPERVISOR',
                    ack_deadline = NULL
                WHERE incident_id = $1;
                """,
                incident_id,
            )

            print(
                f"⚠️ No available responder found "
                f"for incident {incident_id}. "
                f"Escalated to SUPERVISOR."
            )

            return None

        responder_id = best_responder["responder_id"]

        deadline = (
            datetime.now(timezone.utc)
            + timedelta(seconds=ACK_TIMEOUT_SECONDS)
        )

        #
        # Create assignment
        #
        await conn.execute(
            """
            INSERT INTO assignments (
                incident_id,
                responder_id,
                status
            )
            VALUES (
                $1,
                $2,
                'ASSIGNED'
            );
            """,
            incident_id,
            responder_id,
        )

        #
        # Increment responder workload
        #
        await conn.execute(
            """
            UPDATE responders
            SET current_active_incidents =
                current_active_incidents + 1
            WHERE responder_id = $1;
            """,
            responder_id,
        )

        #
        # Start ACK lifecycle
        #
        await conn.execute(
            """
            UPDATE incidents
            SET
                status = 'ASSIGNED',
                assignment_count = 1,
                ack_deadline = $2
            WHERE incident_id = $1;
            """,
            incident_id,
            deadline,
        )

        print(
            f"✅ Incident {incident_id} assigned to "
            f"{best_responder['name']} ({responder_id})"
        )

        return responder_id


async def main():
    pool = await asyncpg.create_pool(
        dsn=DATABASE_URL
    )

    try:
        async with pool.acquire() as conn:

            await route_incident(
                conn,
                "IR-SUB-001",
                lng=-0.12,
                lat=51.50,
            )

    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio