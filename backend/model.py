from pydantic import BaseModel, Field
from typing import Optional, Dict, Any

class IngestRequest(BaseModel):
    video: str = Field(..., description="Absolute path or URL to video")
    metadata: Optional[Dict[str, Any]] = {}

class JobResponse(BaseModel):
    job_id: int
    status: str