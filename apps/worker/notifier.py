import asyncio
import os
import random
import asyncpg

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:postgres_password@localhost:5432/emergency_db",
)


class MockSMSProvider:
    """Simulates an SMS gateway that can fail transiently."""

    def __init__(self, failure_rate: float = 0.0):
        self.failure_rate = failure_rate

    async def send(self, recipient_phone: str, message_body: str) -> bool:
        await asyncio.sleep(0.1)  # Simulate network latency
        if random.random() < self.failure_rate:
            raise RuntimeError("SMS Gateway Connection Timeout")
        print(f"📱 [SMS DELIVERED] To: {recipient_phone} | Body: {message_body}")
        return True


async def process_pending_notifications(
    conn: asyncpg.Connection, provider: MockSMSProvider
):
    """Fetches PENDING or retryable FAILED notifications and delivers them."""
    records = await conn.fetch(
        """
        SELECT notification_id, incident_id, responder_id, recipient_phone, message_body, retry_count, max_retries
        FROM notifications
        WHERE status IN ('PENDING', 'FAILED')
          AND retry_count < max_retries
        ORDER BY created_at ASC
        FOR UPDATE SKIP LOCKED;
        """
    )

    for row in records:
        notif_id = row["notification_id"]
        try:
            await provider.send(row["recipient_phone"], row["message_body"])
            await conn.execute(
                """
                UPDATE notifications
                SET status = 'SENT', sent_at = NOW(), last_error = NULL
                WHERE notification_id = $1;
                """,
                notif_id,
            )
        except Exception as err:
            new_retry = row["retry_count"] + 1
            new_status = (
                "EXHAUSTED" if new_retry >= row["max_retries"] else "FAILED"
            )
            await conn.execute(
                """
                UPDATE notifications
                SET status = $1, retry_count = $2, last_error = $3
                WHERE notification_id = $4;
                """,
                new_status,
                new_retry,
                str(err),
                notif_id,
            )
            print(
                f"⚠️ [SMS FAILED] Notif ID {notif_id} (Attempt {new_retry}/{row['max_retries']}): {err}"
            )


async def run_notification_loop():
    pool = await asyncpg.create_pool(dsn=DATABASE_URL)
    provider = MockSMSProvider(failure_rate=0.3)  # 30% simulated failure rate
    print("⏳ Notification worker started...")
    try:
        while True:
            async with pool.acquire() as conn:
                await process_pending_notifications(conn, provider)
            await asyncio.sleep(2)
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(run_notification_loop())