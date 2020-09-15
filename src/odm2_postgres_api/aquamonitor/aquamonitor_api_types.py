"""
types that map the types in aquamonitor API. This is manually handled so there might be mismatches if API is updated.

The types should match their corresponding implementation in
https://github.com/NIVANorge/AquaMonitor/tree/master/Libraries/Core/Niva.AquaMonitor.Cargo

Using pydantic BaseModel for runtime validation
"""
# https://pydantic-docs.helpmanual.io/usage/postponed_annotations/#self-referencing-models
from __future__ import annotations

from datetime import datetime
from typing import Optional, Dict

from pydantic import Field
from pydantic.main import BaseModel


class StationCargo(BaseModel):
    Id: int
    ProjectId: int
    Type: Dict
    Code: Optional[str] = None
    Name: Optional[str] = None
    Selected: bool


class ProjectCargoCreate(BaseModel):
    name: str = Field(None, alias='_Name')
    description: str = Field(None, alias='_Description')
    number: str = Field(None, alias='_Number')
    start_date: Optional[datetime] = Field(None, alias='_StartDate')
    end_date: Optional[datetime] = Field(None, alias='_EndDate')


class ProjectCargo(ProjectCargoCreate):
    id: int = Field(None, alias='_Id')


class BegroingSampleCargoCreate(BaseModel):
    Station: StationCargo
    SampleDate: datetime
    FieldWorkBy: Optional[str] = None


class BegroingSampleCargo(BegroingSampleCargoCreate):
    Id: int


class MethodCargo(BaseModel):
    Id: int
    Name: Optional[str] = None
    Unit: Optional[str] = None
    Laboratory: Optional[str] = None
    MethodRef: Optional[str] = None
    Matrix: Optional[str] = None


class DomainCargo(BaseModel):
    Id: int
    Name: Optional[str] = None
    Description: Optional[str] = None


class TaxonomicLevelCargo(BaseModel):
    Id: int
    Parent: Optional[TaxonomicLevelCargo] = None
    Name: str


class TaxonomyCargo(BaseModel):
    Id: int
    Parent: Optional[TaxonomyCargo] = None
    Level: Optional[TaxonomicLevelCargo] = None
    LatinName: Optional[str] = None
    NorwegianName: Optional[str] = None
    ArsdatabankID: Optional[int] = None
    Origin: Optional[str] = None


TaxonomyCargo.update_forward_refs()


class TaxonomyCodeCargo(BaseModel):
    Id: int
    Code: Optional[str] = None
    Name: Optional[str] = None
    Domain: DomainCargo
    Taxonomy: TaxonomyCargo
    Autor: Optional[str] = None
    Remark: Optional[str] = None


TaxonomyCodeCargo.update_forward_refs()


class BegroingObservationCargoCreate(BaseModel):
    Sample: BegroingSampleCargo
    Method: MethodCargo
    Taxonomy: TaxonomyCodeCargo
    Cf: Optional[str] = None
    Value: str
    IdentifiedBy: Optional[str] = None
    Remark: Optional[str] = None


class BegroingObservationCargo(BegroingObservationCargoCreate):
    Id: int
