from typing import Optional
from pydantic import BaseModel, Field

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