import uuid
from datetime import timedelta, datetime
from typing import List

from pydantic.main import BaseModel

from odm2_postgres_api.schemas.schemas import SamplingFeatures, Action, FeatureActions


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
    station_sampling_feature: uuid.UUID
    fish_sampling_feature: uuid.UUID
    action: Action


class FishObservationResponse(BaseModel):
    fish_sampling_features: List[SamplingFeatures]
    station_sampling_features: List[SamplingFeatures]
    observations: List[FishObservationStored]
