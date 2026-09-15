import asyncio
import json
import logging
import os

import asyncpg
import redis.asyncio as redis

from apps.worker.router import route_incident

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ingestion_worker")


DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:postgres_password@localhost:5432/emergency_db",
)

REDIS_URL = os.getenv(
    "REDIS_URL",
    "redis://localhost:6379/0",
)

STREAM_NAME = "incidents:ingest"
CONSUMER_GROUP = "incident_processors"
CONSUMER_NAME = "worker_1"


# Temporary coordinates until geocoding is implemented.
DEFAULT_LNG = -0.12
DEFAULT_LAT = 51.50


async def init_consumer_group(redis_client: redis.Redis):
    """
    Ensure the consumer group exists.
    """

    try:
        await redis_client.xgroup_create(
            STREAM_NAME,
            CONSUMER_GROUP,
            id="0",
            mkstream=True,
        )
    except redis.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise


async def process_event(
    db_pool: asyncpg.Pool,
    event_data: dict,
) -> bool:
    """
    Idempotent ingestion pipeline.

    Flow:

    Redis Event
        ↓
    Insert Incident
        ↓
    Route Incident
        ↓
    Create Assignment
        ↓
    Start ACK Timeout Lifecycle
    """

    async with db_pool.acquire() as conn:
        async with conn.transaction():

            result = await conn.execute(
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
                event_data["incident_id"],
                event_data["organization_id"],
                event_data["submission_id"],
                event_data["channel"],
                event_data["category"],
                event_data["urgency"],
                event_data["location_text"],
                event_data["description"],
            )

            #
            # Duplicate submission
            #
            if result == "INSERT 0 0":
                logger.warning(
                    "Duplicate submission ignored: %s",
                    event_data["submission_id"],
                )
                return False

            logger.info(
                "Successfully processed incident: %s",
                event_data["incident_id"],
            )

            #
            # Route immediately.
            #
            await route_incident(
                conn,
                event_data["incident_id"],
                lng=DEFAULT_LNG,
                lat=DEFAULT_LAT,
            )

            return True


async def drain_pg_outbox(
    db_pool: asyncpg.Pool,
):
    """
    Replays durable ingestion events written directly
    to PostgreSQL during Redis outages.
    """

    async with db_pool.acquire() as conn:
        async with conn.transaction():

            records = await conn.fetch(
                """
                SELECT
                    event_id,
                    payload
                FROM ingestion_events
                WHERE status = 'PENDING'
                ORDER BY created_at ASC
                LIMIT 50
                FOR UPDATE SKIP LOCKED;
                """
            )

            for rec in records:

                event_id = rec["event_id"]
                payload = json.loads(rec["payload"])

                try:
                    await process_event(
                        db_pool,
                        payload,
                    )

                    await conn.execute(
                        """
                        UPDATE ingestion_events
                        SET
                            status = 'REPROCESSED',
                            processed_at = NOW()
                        WHERE event_id = $1;
                        """,
                        event_id,
                    )

                except Exception as e:
                    logger.exception(
                        "Outbox replay failed for %s",
                        event_id,
                    )

                    await conn.execute(
                        """
                        UPDATE ingestion_events
                        SET status = 'FAILED'
                        WHERE event_id = $1;
                        """,
                        event_id,
                    )


async def process_stream_entries(
    redis_client: redis.Redis,
    db_pool: asyncpg.Pool,
):
    """
    Reads and processes events from Redis.
    """

    entries = await redis_client.xreadgroup(
        groupname=CONSUMER_GROUP,
        consumername=CONSUMER_NAME,
        streams={STREAM_NAME: ">"},
        count=10,
        block=2000,
    )

    for _, messages in entries:
        for message_id, data in messages:

            payload = json.loads(
                data["payload"]
            )

            try:
                await process_event(
                    db_pool,
                    payload,
                )

                await redis_client.xack(
                    STREAM_NAME,
                    CONSUMER_GROUP,
                    message_id,
                )

            except Exception:
                logger.exception(
                    "Failed processing message %s",
                    message_id,
                )


async def run_worker():
    db_pool = await asyncpg.create_pool(
        dsn=DATABASE_URL,
        min_size=1,
        max_size=10,
    )

    redis_client = redis.from_url(
        REDIS_URL,
        decode_responses=True,
    )

    await init_consumer_group(redis_client)

    logger.info