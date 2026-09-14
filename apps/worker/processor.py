import asyncio
import json
import logging
import os
import asyncpg
import redis.asyncio as redis

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ingestion_worker")

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres_password@localhost:5432/emergency_db")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

STREAM_NAME = "incidents:ingest"
CONSUMER_GROUP = "incident_processors"
CONSUMER_NAME = "worker_1"


async def init_consumer_group(redis_client: redis.Redis):
    """Ensure the consumer group exists on the stream."""
    try:
        await redis_client.xgroup_create(STREAM_NAME, CONSUMER_GROUP, id="0", mkstream=True)
    except redis.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise e


async def process_event(db_pool: asyncpg.Pool, event_data: dict) -> bool:
    """
    Inserts incident into Postgres.
    Uses ON CONFLICT to ignore duplicate submission_ids safely.
    """
    async with db_pool.acquire() as conn:
        result = await conn.execute(
            """
            INSERT INTO incidents (
                incident_id, organization_id, submission_id, channel, 
                category, urgency, status, location_text, description
            )
            VALUES ($1, $2, $3, $4, $5, $6, 'QUEUED', $7, $8)
            ON CONFLICT (organization_id, submission_id) DO NOTHING;
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
        
        if result == "INSERT 0 0":
            logger.warning(f"Duplicate submission detected & ignored: {event_data['submission_id']}")
            return False
        
        logger.info(f"Successfully processed incident: {event_data['incident_id']}")
        return True


async def run_worker():
    db_pool = await asyncpg.create_pool(dsn=DATABASE_URL)
    redis_client = redis.from_url(REDIS_URL, decode_responses=True)
    
    await init_consumer_group(redis_client)
    logger.info("Worker started. Listening for incoming incidents...")

    try:
        while True:
            # Read new messages from the stream
            entries = await redis_client.xreadgroup(
                groupname=CONSUMER_GROUP,
                consumername=CONSUMER_NAME,
                streams={STREAM_NAME: ">"},
                count=10,
                block=2000  # Block for up to 2 seconds waiting for messages
            )

            for stream, messages in entries:
                for message_id, data in messages:
                    payload = json.loads(data["payload"])
                    
                    # Insert into DB idempotently
                    await process_event(db_pool, payload)
                    
                    # Acknowledge stream message so it isn't re-processed
                    await redis_client.xack(STREAM_NAME, CONSUMER_GROUP, message_id)

    finally:
        await db_pool.close()
        await redis_client.close()

if __name__ == "__main__":
    asyncio.run(run_worker())