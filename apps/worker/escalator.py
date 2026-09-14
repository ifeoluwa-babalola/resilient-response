import asyncio
import os
from datetime import datetime, timezone, timedelta
import asyncpg

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres_password@localhost:5432/emergency_db")
ACK_TIMEOUT_SECONDS = 15  # Short timeout for local testing


async def process_timeouts(conn: asyncpg.Connection):
    """
    Scans for incidents that passed their ACK deadline without an acknowledgment.
    Reassigns to another responder, or escalates to SUPERVISOR if max attempts exceeded.
    """
    now = datetime.now(timezone.utc)

    # 1. Fetch unacknowledged timed-out incidents
    stale_incidents = await conn.fetch(
        """
        SELECT incident_id, assignment_count, max_assignments, location
        FROM incidents
        WHERE status = 'ASSIGNED'
          AND ack_deadline IS NOT NULL
          AND ack_deadline <= $1
        FOR UPDATE;
        """,
        now
    )

    for record in stale_incidents:
        incident_id = record["incident_id"]
        count = record["assignment_count"]
        max_attempts = record["max_assignments"]

        # Mark previous active assignment as TIMED_OUT
        await conn.execute(
            """
            UPDATE assignments 
            SET status = 'TIMED_OUT', timed_out_at = $1 
            WHERE incident_id = $2 AND status = 'ASSIGNED';
            """,
            now, incident_id
        )

        # Reclaim active workload from previous assigned responder
        await conn.execute(
            """
            UPDATE responders 
            SET current_active_incidents = GREATEST(0, current_active_incidents - 1)
            WHERE responder_id IN (
                SELECT responder_id FROM assignments 
                WHERE incident_id = $1 AND status = 'TIMED_OUT'
                ORDER BY assigned_at DESC LIMIT 1
            );
            """,
            incident_id
        )

        if count >= max_attempts:
            # Escalation threshold reached -> Pass to Supervisor
            await conn.execute(
                """
                UPDATE incidents 
                SET status = 'ESCALATED_TO_SUPERVISOR', ack_deadline = NULL 
                WHERE incident_id = $1;
                """,
                incident_id
            )
            print(f"🚨 INCIDENT {incident_id}: Escalated to SUPERVISOR (Exceeded {max_attempts} responder attempts)")

        else:
            # Find next eligible candidate (excluding previously timed-out responders)
            next_responder = await conn.fetchrow(
                """
                SELECT responder_id, name
                FROM responders
                WHERE is_eligible = TRUE
                  AND is_online = TRUE
                  AND current_active_incidents < active_capacity
                  AND responder_id NOT IN (
                      SELECT responder_id FROM assignments WHERE incident_id = $1
                  )
                ORDER BY current_active_incidents ASC
                LIMIT 1;
                """,
                incident_id
            )

            if next_responder:
                resp_id = next_responder["responder_id"]
                new_deadline = now + timedelta(seconds=ACK_TIMEOUT_SECONDS)

                # Assign to next responder
                await conn.execute(
                    """
                    INSERT INTO assignments (incident_id, responder_id, status)
                    VALUES ($1, $2, 'ASSIGNED');
                    """,
                    incident_id, resp_id
                )

                await conn.execute(
                    """
                    UPDATE responders 
                    SET current_active_incidents = current_active_incidents + 1 
                    WHERE responder_id = $1;
                    """,
                    resp_id
                )

                await conn.execute(
                    """
                    UPDATE incidents 
                    SET status = 'ASSIGNED', 
                        assignment_count = assignment_count + 1, 
                        ack_deadline = $1 
                    WHERE incident_id = $2;
                    """,
                    new_deadline, incident_id
                )
                print(f"🔁 INCIDENT {incident_id}: Timeout. Re-assigned to {next_responder['name']} ({resp_id}) [Attempt {count + 1}]")
            else:
                # No other responders available -> Direct Supervisor Escalation
                await conn.execute(
                    """
                    UPDATE incidents 
                    SET status = 'ESCALATED_TO_SUPERVISOR', ack_deadline = NULL 
                    WHERE incident_id = $1;
                    """,
                    incident_id
                )
                print(f"🚨 INCIDENT {incident_id}: No other available responders. Escalated to SUPERVISOR.")


async def run_escalation_loop():
    pool = await asyncpg.create_pool(dsn=DATABASE_URL)
    print("⏳ Escalation monitor loop started running every 3 seconds...")
    try:
        while True:
            async with pool.acquire() as conn:
                await process_timeouts(conn)
            await asyncio.sleep(3)
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(run_escalation_loop())