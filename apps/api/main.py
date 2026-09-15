import os
from datetime import datetime, timezone
from contextlib import asynccontextmanager

import redis.asyncio as redis

from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel

from apps.db import init_db, close_db, get_db
from apps.routes import incidents, dashboard


REDIS_URL = os.getenv(
    "REDIS_URL",
    "redis://localhost:6379/0",
)


# -------------------------------------------------------
# Application Lifecycle
# -------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    yield
    await close_db()


app = FastAPI(
    title="Resilient Response API",
    lifespan=lifespan,
)


app.include_router(incidents.router)
app.include_router(dashboard.router)


# -------------------------------------------------------
# Startup / Shutdown
# -------------------------------------------------------

@app.on_event("startup")
async def startup():
    app.state.redis = redis.from_url(
        REDIS_URL,
        decode_responses=True,
    )


@app.on_event("shutdown")
async def shutdown():
    await app.state.redis.aclose()


# -------------------------------------------------------
# Health
# -------------------------------------------------------

@app.get("/health")
async def health_check():
    return {"status": "ok"}


# -------------------------------------------------------
# ACK Models
# -------------------------------------------------------

class IncidentAckRequest(BaseModel):
    responder_id: str


# -------------------------------------------------------
# ACK Endpoint
# -------------------------------------------------------

@app.post(
    "/v1/incidents/{incident_id}/ack",
    status_code=status.HTTP_200_OK,
)
async def acknowledge_incident(
    incident_id: str,
    payload: IncidentAckRequest,
):
    """
    Responder acknowledges assignment.

    Final valid state:

        assignment -> ACKNOWLEDGED
        incident   -> IN_PROGRESS

    This endpoint intentionally locks the incident row
    first so it uses the same lock ordering strategy
    as the timeout/escalation worker.
    """

    db_pool = get_db()

    if db_pool is None:
        raise HTTPException(
            status_code=500,
            detail="Database pool not initialized.",
        )

    now = datetime.now(timezone.utc)

    async with db_pool.acquire() as conn:
        async with conn.transaction():

            # -------------------------------------------------
            # Lock incident first.
            # Timeout worker should lock incident first too.
            # Prevents ACK/TIMEOUT deadlocks.
            # -------------------------------------------------

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
                raise HTTPException(
                    status_code=404,
                    detail="Incident not found.",
                )

            # -------------------------------------------------
            # ACK assignment
            # -------------------------------------------------

            result = await conn.execute(
                """
                UPDATE assignments
                SET
                    status = 'ACKNOWLEDGED',
                    ack_at = $1
                WHERE incident_id = $2
                  AND responder_id = $3
                  AND status = 'ASSIGNED';
                """,
                now,
                incident_id,
                payload.responder_id,
            )

            if result == "UPDATE 0":
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "Assignment not found, already "
                        "acknowledged, or timed out."
                    ),
                )

            # -------------------------------------------------
            # Move incident into active state
            # -------------------------------------------------

            await conn.execute(
                """
                UPDATE incidents
                SET
                    status = 'IN_PROGRESS',
                    ack_deadline = NULL
                WHERE incident_id = $1
                  AND status = 'ASSIGNED';
                """,
                incident_id,
            )

            return {
                "status": "SUCCESS",
                "incident_id": incident_id,
                "state": "IN_PROGRESS",
            }