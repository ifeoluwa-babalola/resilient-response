import os
import uuid
from datetime import datetime, timezone
from typing import Optional

import asyncpg
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres_password@localhost:5432/emergency_db")
DEFAULT_ORG_ID = uuid.UUID("00000000-0000-0000-0000-000000000001")

app = FastAPI(title="Resilient Response API")


# Database Pool Management
@app.on_event("startup")
async def startup():
    app.state.db_pool = await asyncpg.create_pool(dsn=DATABASE_URL)


@app.on_event("shutdown")
async def shutdown():
    await app.state.db_pool.close()


# Pydantic Input Model
class IncidentIngestionRequest(BaseModel):
    submission_id: str = Field(..., example="SUB-001")
    channel: str = Field(..., example="USSD")
    category: str = Field(..., example="SEXUAL_VIOLENCE")
    urgency: str = Field(..., example="HIGH")
    location_text: str = Field(..., example="Central Market")
    description: Optional[str] = Field(None, example="Incident reported near Central Market")


# Response Model
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
    now = datetime.now(timezone.utc)

    async with app.state.db_pool.acquire() as conn:
        try:
            # Insert incident directly into PostgreSQL
            await conn.execute(
                """
                INSERT INTO incidents (
                    incident_id, organization_id, submission_id, channel, 
                    category, urgency, status, location_text, description, received_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, 'QUEUED', $7, $8, $9)
                """,
                incident_id,
                DEFAULT_ORG_ID,
                payload.submission_id,
                payload.channel,
                payload.category,
                payload.urgency,
                payload.location_text,
                payload.description,
                now,
            )

            return IncidentIngestionResponse(
                status="ACCEPTED",
                submission_id=payload.submission_id,
                incident_id=incident_id,
            )

        except asyncpg.UniqueViolationError:
            # Handle duplicate submission cleanly without creating a second record
            return IncidentIngestionResponse(
                status="ALREADY_ACCEPTED",
                submission_id=payload.submission_id,
                incident_id=incident_id,
            )