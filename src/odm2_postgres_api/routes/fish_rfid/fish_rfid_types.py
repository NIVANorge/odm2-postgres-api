from datetime import timedelta, datetime
from typing import List

from pydantic.main import BaseModel

from odm2_postgres_api.schemas.schemas import SamplingFeatures, Action


class FishObservationCreate(BaseModel):
    fish_tag: str
    datetime: datetime
    station_code: str
    duration: timedelta
    field_strength: float
    consecutive_detections: int


class FishObservationRequest(BaseModel):
    observations: List[FishObservationCreate]


class FishObservationStored(BaseModel):
    action: Action
    fish_sampling_feature: SamplingFeatures
    station_sampling_feature: SamplingFeatures


class FishObservationResponse(BaseModel):
    observations: List[FishObservationStored]
