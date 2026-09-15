import json
import os
from fastapi import APIRouter, status, Depends
from pydantic import BaseModel, Field
import redis.asyncio as redis
import asyncpg

router = APIRouter(prefix="/v1/incidents", tags=["Incidents"])

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
STREAM_NAME = "incidents:ingest"


class IncidentIngestionRequest(BaseModel):
    submission_id: str
    channel: str
    category: str
    urgency: str
    location_text: str
    description: str | None = None


class IncidentIngestionResponse(BaseModel):
    status: str
    submission_id: str
    incident_id: str


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=IncidentIngestionResponse,
)
async def ingest_incident(payload: IncidentIngestionRequest, req_app=None):
    incident_id = f"IR-{payload.submission_id.upper()}"
    organization_id = "00000000-0000-0000-0000-000000000001"

    event_data = {
        "incident_id": incident_id,
        "organization_id": organization_id,
        "submission_id": payload.submission_id,
        "channel": payload.channel,
        "category": payload.category,
        "urgency": payload.urgency,
        "location_text": payload.location_text,
        "description": payload.description or "",
    }

    pushed_to_redis = False

    # Attempt Fast Path: Write to Redis Stream
    try:
        r = redis.from_url(REDIS_URL, decode_responses=True)
        await r.xadd(STREAM_NAME, {"payload": json.dumps(event_data)})
        await r.close()
        pushed_to_redis = True
    except Exception:
        # Redis dead/unreachable -> Fall through to Postgres buffer
        pushed_to_redis = False

    if not pushed_to_redis:
        # Fallback Path: Direct write into PostgreSQL ingestion_events buffer
        db_url = os.getenv(
            "DATABASE_URL",
            "postgresql://postgres:postgres_password@localhost:5432/emergency_db",
        )
        conn = await asyncpg.connect(db_url)
        try:
            await conn.execute(
                """
                INSERT INTO ingestion_events (submission_id, payload, status)
                VALUES ($1, $2, 'PENDING');
                """,
                payload.submission_id,
                json.dumps(event_data),
            )
        finally:
            await conn.close()

    return IncidentIngestionResponse(
        status="ACCEPTED",
        submission_id=payload.submission_id,
        incident_id=incident_id,
    )