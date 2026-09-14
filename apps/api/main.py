import json
import os
import uuid
from typing import Optional

from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field
import redis.asyncio as redis

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
STREAM_NAME = "incidents:ingest"

app = FastAPI(title="Resilient Response API")


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