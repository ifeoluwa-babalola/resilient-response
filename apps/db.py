import os
import asyncpg

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:postgres_password@localhost:5432/emergency_db",
)

pool: asyncpg.Pool | None = None


async def init_db():
    global pool

    if pool is None:
        pool = await asyncpg.create_pool(
            dsn=DATABASE_URL,
            min_size=1,
            max_size=10,
        )


async def close_db():
    global pool

    if pool:
        await pool.close()
        pool = None


def get_db():
    return pool