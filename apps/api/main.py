import json
import os
import uuid
from typing import Optional

from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, status
from apps.db import init_db, close_db
from apps.routes import incidents
from pydantic import BaseModel, Field
import redis.asyncio as redis


REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
STREAM_NAME = "incidents:ingest"

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    yield
    await close_db()

app = FastAPI(title="Resilient Response API", lifespan=lifespan)

app.include_router(incidents.router)

@app.get("/health")
async def health_check():
    return {"status": "ok"}

@app.on_event("startup")
async def startup():
    app.state.redis = redis.from_url(REDIS_URL, decode_responses=True)


@app.on_event("shutdown")
async def shutdown():
    await app.state.redis.close()


class IncidentIngestionRequest(BaseModel):
    submission_id: str = Field(..., example="SUB-001")
    channel: str = Field(..., example="USSD")
    category: str = Field(..., example="SEXUAL_VIOLENCE")
    urgency: str = Field(..., example="HIGH")
    location_text: str = Field(..., example="Central Market")
    description: Optional[str] = Field(None, example="Incident reported near Central Market")


class IncidentIngestionResponse(BaseModel):
    status: str
    submission_id: str
    incident_id: str


@app.post(
    "/v1/incidents",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=IncidentIngestionResponse,
)
async def ingest_incident(payload: IncidentIngestionRequest):
    incident_id = f"IR-{payload.submission_id.upper()}"
    organization_id = "00000000-0000-0000-0000-000000000001"

    # Serialize payload for stream message
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

    # Push to Redis Stream using XADD
    await app.state.redis.xadd(
        STREAM_NAME,
        {"payload": json.dumps(event_data)}
    )

    return IncidentIngestionResponse(
        status="ACCEPTED",
        submission_id=payload.submission_id,
        incident_id=incident_id,
    )

# Minimal ACK request model
class IncidentAckRequest(BaseModel):
    responder_id: str


@app.post("/v1/incidents/{incident_id}/ack", status_code=status.HTTP_200_OK)
async def acknowledge_incident(incident_id: str, payload: IncidentAckRequest):
    """Simulates a responder clicking ACK on their dashboard."""
    now = datetime.now(timezone.utc)
    
    async with app.state.db_pool.acquire() as conn:
        result = await conn.execute(
            """
            UPDATE assignments 
            SET status = 'ACKNOWLEDGED', ack_at = $1 
            WHERE incident_id = $2 AND responder_id = $3 AND status = 'ASSIGNED';
            """,
            now, incident_id, payload.responder_id
        )

        if result == "UPDATE 0":
            raise HTTPException(status_code=400, detail="Assignment not found, already acknowledged, or timed out.")

        await conn.execute(
            """
            UPDATE incidents 
            SET status = 'IN_PROGRESS', ack_deadline = NULL 
            WHERE incident_id = $1;
            """,
            incident_id
        )

        return {"status": "SUCCESS", "incident_id": incident_id, "state": "IN_PROGRESS"}