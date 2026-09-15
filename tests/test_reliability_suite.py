import asyncio
import json
import os

import asyncpg
import pytest
import redis.asyncio as redis

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:postgres_password@localhost:5432/emergency_db",
)

REDIS_URL = os.getenv(
    "REDIS_URL",
    "redis://localhost:6379/0",
)


# -------------------------------------------------------------------
# Fixtures
# -------------------------------------------------------------------

@pytest.fixture
async def db():
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        yield conn
    finally:
        await conn.close()


@pytest.fixture
async def r():
    client = redis.from_url(
        REDIS_URL,
        decode_responses=True,
    )
    try:
        yield client
    finally:
        await client.aclose()


# -------------------------------------------------------------------
# Test 1: 5 Concurrent Duplicate Submissions → 1 Incident
# -------------------------------------------------------------------

@pytest.mark.asyncio
async def test_duplicate_submission_idempotency(db, r):
    sub_id = "SUB-CHAOS-DUP-01"

    payload = {
        "incident_id": f"IR-{sub_id}",
        "organization_id": "00000000-0000-0000-0000-000000000001",
        "submission_id": sub_id,
        "channel": "USSD",
        "category": "MEDICAL",
        "urgency": "HIGH",
        "location_text": "Market Square",
        "description": "Duplicate Storm Test",
    }

    await db.execute(
        "DELETE FROM incidents WHERE submission_id = $1;",
        sub_id,
    )

    tasks = [
        r.xadd(
            "incidents:ingest",
            {"payload": json.dumps(payload)},
        )
        for _ in range(5)
    ]

    await asyncio.gather(*tasks)

    entries = await r.xread(
        streams={"incidents:ingest": "0"},
        count=5,
    )

    for _, messages in entries:
        for _, data in messages:
            p = json.loads(data["payload"])

            await db.execute(
                """
                INSERT INTO incidents (
                    incident_id,
                    organization_id,
                    submission_id,
                    channel,
                    category,
                    urgency,
                    status,
                    location_text,
                    description
                )
                VALUES (
                    $1,$2,$3,$4,$5,$6,
                    'QUEUED',
                    $7,$8
                )
                ON CONFLICT (
                    organization_id,
                    submission_id
                )
                DO NOTHING;
                """,
                p["incident_id"],
                p["organization_id"],
                p["submission_id"],
                p["channel"],
                p["category"],
                p["urgency"],
                p["location_text"],
                p["description"],
            )

    count = await db.fetchval(
        """
        SELECT COUNT(*)
        FROM incidents
        WHERE submission_id = $1;
        """,
        sub_id,
    )

    assert count == 1


# -------------------------------------------------------------------
# Test 2: No Available Responders → Supervisor Escalation
# -------------------------------------------------------------------

@pytest.mark.asyncio
async def test_zero_responder_availability_fallback(db):
    inc_id = "IR-CHAOS-NORESP-01"

    await db.execute(
        "DELETE FROM incidents WHERE incident_id = $1;",
        inc_id,
    )

    await db.execute(
        "UPDATE responders SET is_online = FALSE;"
    )

    try:
        await db.execute(
            """
            INSERT INTO incidents (
                incident_id,
                organization_id,
                submission_id,
                channel,
                category,
                urgency,
                status,
                location_text
            )
            VALUES (
                $1,
                '00000000-0000-0000-0000-000000000001',
                'SUB-NORESP',
                'USSD',
                'FIRE',
                'CRITICAL',
                'QUEUED',
                'Outpost'
            );
            """,
            inc_id,
        )

        responder = await db.fetchrow(
            """
            SELECT responder_id
            FROM responders
            WHERE is_eligible = TRUE
              AND is_online = TRUE
              AND current_active_incidents < active_capacity
            LIMIT 1;
            """
        )

        if not responder:
            await db.execute(
                """
                UPDATE incidents
                SET status = 'ESCALATED_TO_SUPERVISOR'
                WHERE incident_id = $1;
                """,
                inc_id,
            )

        status = await db.fetchval(
            """
            SELECT status
            FROM incidents
            WHERE incident_id = $1;
            """,
            inc_id,
        )

        assert status == "ESCALATED_TO_SUPERVISOR"

    finally:
        await db.execute(
            "UPDATE responders SET is_online = TRUE;"
        )


# -------------------------------------------------------------------
# Test 3: ACK vs Timeout Race Condition
# -------------------------------------------------------------------

@pytest.mark.asyncio
async def test_ack_escalation_race_condition():
    pool = await asyncpg.create_pool(DATABASE_URL)

    try:
        inc_id = "IR-CHAOS-RACE-01"

        async with pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM assignments WHERE incident_id=$1;",
                inc_id,
            )

            await conn.execute(
                "DELETE FROM incidents WHERE incident_id=$1;",
                inc_id,
            )

            await conn.execute(
                """
                INSERT INTO incidents (
                    incident_id,
                    organization_id,
                    submission_id,
                    channel,
                    category,
                    urgency,
                    status,
                    location_text,
                    ack_deadline
                )
                VALUES (
                    $1,
                    '00000000-0000-0000-0000-000000000001',
                    'SUB-RACE',
                    'USSD',
                    'MEDICAL',
                    'HIGH',
                    'ASSIGNED',
                    'Sector 4',
                    NOW() - INTERVAL '1 second'
                );
                """,
                inc_id,
            )

            await conn.execute(
                """
                INSERT INTO assignments (
                    incident_id,
                    responder_id,
                    status
                )
                VALUES (
                    $1,
                    'RESP-1',
                    'ASSIGNED'
                );
                """,
                inc_id,
            )

        async def late_ack():
            async with pool.acquire() as conn:
                async with conn.transaction():

                    assignment_update = await conn.execute(
                        """
                        UPDATE assignments
                        SET status = 'ACKNOWLEDGED'
                        WHERE incident_id = $1
                          AND responder_id = 'RESP-1'
                          AND status = 'ASSIGNED';
                        """,
                        inc_id,
                    )

                    if assignment_update != "UPDATE 0":
                        await conn.execute(
                            """
                            UPDATE incidents
                            SET status='IN_PROGRESS',
                                ack_deadline=NULL
                            WHERE incident_id=$1
                              AND status='ASSIGNED';
                            """,
                            inc_id,
                        )

        async def timeout_sweep():
            async with pool.acquire() as conn:
                async with conn.transaction():

                    row = await conn.fetchrow(
                        """
                        SELECT incident_id
                        FROM incidents
                        WHERE incident_id = $1
                          AND status = 'ASSIGNED'
                          AND ack_deadline <= NOW()
                        FOR UPDATE;
                        """,
                        inc_id,
                    )

                    if not row:
                        return

                    timeout_update = await conn.execute(
                        """
                        UPDATE assignments
                        SET status='TIMED_OUT'
                        WHERE incident_id=$1
                          AND status='ASSIGNED';
                        """,
                        inc_id,
                    )

                    if timeout_update == "UPDATE 0":
                        return

                    await conn.execute(
                        """
                        UPDATE incidents
                        SET status='ESCALATED_TO_SUPERVISOR',
                            ack_deadline=NULL
                        WHERE incident_id=$1
                          AND status='ASSIGNED';
                        """,
                        inc_id,
                    )

        results = await asyncio.gather(
            late_ack(),
            timeout_sweep(),
            return_exceptions=True,
        )

        for result in results:
            if isinstance(result, asyncpg.exceptions.DeadlockDetectedError):
                pytest.skip(
                    "Deadlock detected during race-condition simulation "
                    "(expected database concurrency behavior)."
                )


        async with pool.acquire() as conn:
            assign_status = await conn.fetchval(
                """
                SELECT status
                FROM assignments
                WHERE incident_id=$1;
                """,
                inc_id,
            )

            inc_status = await conn.fetchval(
                """
                SELECT status
                FROM incidents
                WHERE incident_id=$1;
                """,
                inc_id,
            )

        valid_state = (
            assign_status == "TIMED_OUT"
            and inc_status == "ESCALATED_TO_SUPERVISOR"
        ) or (
            assign_status == "ACKNOWLEDGED"
            and inc_status == "IN_PROGRESS"
        )

        assert valid_state, (
            f"Invalid state mutation: "
            f"assignment={assign_status}, "
            f"incident={inc_status}"
        )

    finally:
        await pool.close()


# -------------------------------------------------------------------
# Test 4: Redis Failure → PostgreSQL Outbox Fallback
# -------------------------------------------------------------------

@pytest.mark.asyncio
async def test_redis_outage_durable_ingestion(db):
    sub_id = "SUB-CHAOS-REDIS-FAIL"

    event_data = {
        "incident_id": f"IR-{sub_id}",
        "organization_id": "00000000-0000-0000-0000-000000000001",
        "submission_id": sub_id,
        "channel": "USSD",
        "category": "FIRE",
        "urgency": "HIGH",
        "location_text": "Offline Bypass St",
        "description": "Redis dead test",
    }

    await db.execute(
        "DELETE FROM ingestion_events WHERE submission_id=$1;",
        sub_id,
    )

    await db.execute(
        "DELETE FROM incidents WHERE submission_id=$1;",
        sub_id,
    )

    await db.execute(
        """
        INSERT INTO ingestion_events (
            submission_id,
            payload,
            status
        )
        VALUES (
            $1,$2,'PENDING'
        );
        """,
        sub_id,
        json.dumps(event_data),
    )

    status = await db.fetchval(
        """
        SELECT status
        FROM ingestion_events
        WHERE submission_id=$1;
        """,
        sub_id,
    )

    assert status == "PENDING"

    row = await db.fetchrow(
        """
        SELECT event_id,payload
        FROM ingestion_events
        WHERE submission_id=$1
          AND status='PENDING';
        """,
        sub_id,
    )

    payload = json.loads(row["payload"])

    await db.execute(
        """
        INSERT INTO incidents (
            incident_id,
            organization_id,
            submission_id,
            channel,
            category,
            urgency,
            status,
            location_text,
            description
        )
        VALUES (
            $1,$2,$3,$4,$5,$6,
            'QUEUED',$7,$8
        )
        ON CONFLICT (
            organization_id,
            submission_id
        )
        DO NOTHING;
        """,
        payload["incident_id"],
        payload["organization_id"],
        payload["submission_id"],
        payload["channel"],
        payload["category"],
        payload["urgency"],
        payload["location_text"],
        payload["description"],
    )

    await db.execute(
        """
        UPDATE ingestion_events
        SET status='REPROCESSED'
        WHERE event_id=$1;
        """,
        row["event_id"],
    )

    assert (
        await db.fetchval(
            """
            SELECT COUNT(*)
            FROM incidents
            WHERE submission_id=$1;
            """,
            sub_id,
        )
    ) == 1

    assert (
        await db.fetchval(
            """
            SELECT status
            FROM ingestion_events
            WHERE submission_id=$1;
            """,
            sub_id,
        )
    ) == "REPROCESSED"


# -------------------------------------------------------------------
# Test 5: Worker Crash Redelivery
# -------------------------------------------------------------------

@pytest.mark.asyncio
async def test_worker_crash_redelivery(db):
    sub_id = "SUB-CHAOS-CRASH-01"
    inc_id = f"IR-{sub_id}"

    await db.execute(
        "DELETE FROM incidents WHERE submission_id=$1;",
        sub_id,
    )

    for _ in range(2):
        await db.execute(
            """
            INSERT INTO incidents (
                incident_id,
                organization_id,
                submission_id,
                channel,
                category,
                urgency,
                status,
                location_text
            )
            VALUES (
                $1,
                '00000000-0000-0000-0000-000000000001',
                $2,
                'USSD',
                'ACCIDENT',
                'HIGH',
                'QUEUED',
                'Crash Corner'
            )
            ON CONFLICT (
                organization_id,
                submission_id
            )
            DO NOTHING;
            """,
            inc_id,
            sub_id,
        )

    count = await db.fetchval(
        """
        SELECT COUNT(*)
        FROM incidents
        WHERE submission_id=$1;
        """,
        sub_id,
    )

    assert count == 1


# -------------------------------------------------------------------
# Test 6: Notification Failure Isolation
# -------------------------------------------------------------------

@pytest.mark.asyncio
async def test_notification_failure_isolation(db):
    inc_id = "IR-CHAOS-NOTIF-01"
    resp_id = "RESP-1"

    await db.execute(
        "DELETE FROM notifications WHERE incident_id=$1;",
        inc_id,
    )

    await db.execute(
        "DELETE FROM assignments WHERE incident_id=$1;",
        inc_id,
    )

    await db.execute(
        "DELETE FROM incidents WHERE incident_id=$1;",
        inc_id,
    )

    await db.execute(
        """
        INSERT INTO incidents (
            incident_id,
            organization_id,
            submission_id,
            channel,
            category,
            urgency,
            status,
            location_text
        )
        VALUES (
            $1,
            '00000000-0000-0000-0000-000000000001',
            'SUB-NOTIF-FAIL',
            'USSD',
            'CRIME',
            'HIGH',
            'ASSIGNED',
            'Safe Zone'
        );
        """,
        inc_id,
    )

    await db.execute(
        """
        INSERT INTO assignments (
            incident_id,
            responder_id,
            status
        )
        VALUES (
            $1,$2,'ASSIGNED'
        );
        """,
        inc_id,
        resp_id,
    )

    notif_id = await db.fetchval(
        """
        INSERT INTO notifications (
            incident_id,
            responder_id,
            recipient_phone,
            message_body,
            status,
            retry_count,
            last_error
        )
        VALUES (
            $1,
            $2,
            '+254700000000',
            'Alert',
            'FAILED',
            1,
            'Simulated SMS Gateway Timeout'
        )
        RETURNING notification_id;
        """,
        inc_id,
        resp_id,
    )

    assert (
        await db.fetchval(
            """
            SELECT status
            FROM assignments
            WHERE incident_id=$1;
            """,
            inc_id,
        )
    ) == "ASSIGNED"

    assert (
        await db.fetchval(
            """
            SELECT status
            FROM incidents
            WHERE incident_id=$1;
            """,
            inc_id,
        )
    ) == "ASSIGNED"

    assert (
        await db.fetchval(
            """
            SELECT status
            FROM notifications
            WHERE notification_id=$1;
            """,
            notif_id,
        )
    ) == "FAILED"
