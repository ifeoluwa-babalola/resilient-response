import asyncio
import os
from datetime import datetime, timezone, timedelta

import asyncpg

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:postgres_password@localhost:5432/emergency_db",
)

ACK_TIMEOUT_SECONDS = 15


async def process_timeouts(conn: asyncpg.Connection):
    """
    Handles expired responder ACK deadlines.

    Valid outcomes:

    1.
        ASSIGNED
            ->
        TIMED_OUT
            ->
        REASSIGNED

    2.
        ASSIGNED
            ->
        TIMED_OUT
            ->
        ESCALATED_TO_SUPERVISOR

    If ACK already won the race,
    escalation immediately exits.
    """

    now = datetime.now(timezone.utc)

    stale_incidents = await conn.fetch(
        """
        SELECT
            incident_id,
            assignment_count,
            max_assignments
        FROM incidents
        WHERE status = 'ASSIGNED'
          AND ack_deadline IS NOT NULL
          AND ack_deadline <= $1
        """,
        now,
    )

    for record in stale_incidents:

        incident_id = record["incident_id"]
        count = record["assignment_count"]
        max_attempts = record["max_assignments"]

        async with conn.transaction():

            # -----------------------------------------
            # Lock incident first.
            # ACK endpoint should do the same.
            # -----------------------------------------

            locked_incident = await conn.fetchrow(
                """
                SELECT incident_id
                FROM incidents
                WHERE incident_id = $1
                  AND status = 'ASSIGNED'
                  AND ack_deadline IS NOT NULL
                  AND ack_deadline <= $2
                FOR UPDATE;
                """,
                incident_id,
                now,
            )

            if not locked_incident:
                continue

            # -----------------------------------------
            # Convert ASSIGNED -> TIMED_OUT
            # -----------------------------------------

            update_result = await conn.execute(
                """
                UPDATE assignments
                SET
                    status = 'TIMED_OUT',
                    timed_out_at = $1
                WHERE incident_id = $2
                  AND status = 'ASSIGNED';
                """,
                now,
                incident_id,
            )

            # ACK already won.
            if update_result == "UPDATE 0":
                continue

            # -----------------------------------------
            # Reclaim workload from timed-out responder
            # -----------------------------------------

            await conn.execute(
                """
                UPDATE responders
                SET current_active_incidents =
                    GREATEST(
                        0,
                        current_active_incidents - 1
                    )
                WHERE responder_id IN (
                    SELECT responder_id
                    FROM assignments
                    WHERE incident_id = $1
                      AND status = 'TIMED_OUT'
                    ORDER BY assigned_at DESC
                    LIMIT 1
                );
                """,
                incident_id,
            )

            # -----------------------------------------
            # Escalation threshold reached
            # -----------------------------------------

            if count >= max_attempts:

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
                    f"🚨 INCIDENT {incident_id}: "
                    f"Escalated to SUPERVISOR "
                    f"(Exceeded {max_attempts} attempts)"
                )

                continue

            # -----------------------------------------
            # Find replacement responder
            # -----------------------------------------

            next_responder = await conn.fetchrow(
                """
                SELECT responder_id, name
                FROM responders
                WHERE is_eligible = TRUE
                  AND is_online = TRUE
                  AND current_active_incidents < active_capacity
                  AND responder_id NOT IN (
                      SELECT responder_id
                      FROM assignments
                      WHERE incident_id = $1
                  )
                ORDER BY current_active_incidents ASC
                LIMIT 1;
                """,
                incident_id,
            )

            if not next_responder:

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
                    f"🚨 INCIDENT {incident_id}: "
                    f"No responders available. "
                    f"Escalated to SUPERVISOR."
                )

                continue

            resp_id = next_responder["responder_id"]

            new_deadline = (
                now
                + timedelta(seconds=ACK_TIMEOUT_SECONDS)
            )

            # -----------------------------------------
            # Create new assignment
            # -----------------------------------------

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
                resp_id,
            )

            await conn.execute(
                """
                UPDATE responders
                SET current_active_incidents =
                    current_active_incidents + 1
                WHERE responder_id = $1;
                """,
                resp_id,
            )

            await conn.execute(
                """
                UPDATE incidents
                SET
                    status = 'ASSIGNED',
                    assignment_count = assignment_count + 1,
                    ack_deadline = $1
                WHERE incident_id = $2;
                """,
                new_deadline,
                incident_id,
            )

            print(
                f"🔁 INCIDENT {incident_id}: "
                f"Re-assigned to "
                f"{next_responder['name']} "
                f"({resp_id}) "
                f"[Attempt {count + 1}]"
            )


async def run_escalation_loop():
    pool = await asyncpg.create_pool(
        dsn=DATABASE_URL,
        min_size=1,
        max_size=10,
    )

    print(
        "⏳ Escalation monitor loop "
        "started running every 3 seconds..."
    )

    try:
        while True:
            async with pool.acquire() as conn:
                await process_timeouts(conn)

            await asyncio.sleep(3)

    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(run_escalation_loop())