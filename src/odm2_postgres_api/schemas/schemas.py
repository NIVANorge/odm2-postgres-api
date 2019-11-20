import uuid
import datetime
from typing import Optional, List, Tuple

import shapely.wkt
from pydantic import BaseModel, BaseConfig, constr, validator

from odm2_postgres_api.queries.controlled_vocabulary_queries import CONTROLLED_VOCABULARY_TABLE_NAMES

cv_checker = constr(regex=f'({"|".join([f"^{cv}$" for cv in CONTROLLED_VOCABULARY_TABLE_NAMES])})')


class PeopleCreate(BaseModel):
    personfirstname: constr(max_length=255)  # type: ignore
    personmiddlename: constr(max_length=255) = None  # type: ignore
    personlastname: constr(max_length=255)  # type: ignore


class People(PeopleCreate):
    personid: int


class OrganizationsCreate(BaseModel):
    organizationtypecv: constr(max_length=255)  # type: ignore
    organizationcode: constr(max_length=50)  # type: ignore
    organizationname: constr(max_length=255)  # type: ignore
    organizationdescription: constr(max_length=5000) = None  # type: ignore
    organizationlink: constr(max_length=255) = None  # type: ignore
    parentorganizationid: Optional[int] = None


class Organizations(OrganizationsCreate):
    organizationid: int


class AffiliationsCreate(BaseModel):
    personid: int
    organizationid: Optional[int] = None
    isprimaryorganizationcontact: Optional[bool] = None
    affiliationstartdate: datetime.date
    affiliationenddate: Optional[datetime.date] = None
    primaryphone: constr(max_length=50) = None  # type: ignore
    primaryemail: constr(max_length=255)  # type: ignore
    primaryaddress: constr(max_length=255) = None  # type: ignore
    personlink: constr(max_length=255) = None  # type: ignore


class Affiliations(AffiliationsCreate):
    affiliationid: int


class MethodsCreate(BaseModel):
    methodtypecv: constr(max_length=255)  # type: ignore
    methodcode: constr(max_length=50)  # type: ignore
    methodname: constr(max_length=255)  # type: ignore
    methoddescription: constr(max_length=5000) = None  # type: ignore
    methodlink: constr(max_length=255) = None  # type: ignore
    organizationid: Optional[int] = None


class Methods(MethodsCreate):
    methodid: int


class ActionsByFields(BaseModel):
    affiliationid: int
    isactionlead: bool
    roledescription: constr(max_length=5000) = None  # type: ignore


class ActionsByCreate(ActionsByFields):
    actionid: int


class ActionsBy(ActionsByCreate):
    bridgeid: int


class ActionsCreate(ActionsByFields):
    actiontypecv: constr(max_length=255)  # type: ignore
    methodid: int
    begindatetime: datetime.datetime
    begindatetimeutcoffset: int
    enddatetime: Optional[datetime.datetime] = None
    enddatetimeutcoffset: Optional[int] = None
    actiondescription: constr(max_length=5000) = None  # type: ignore
    actionfilelink: constr(max_length=255) = None  # type: ignore


class Action(ActionsCreate):
    actionid: int


class SamplingFeaturesCreate(BaseModel):
    class Config(BaseConfig):
        arbitrary_types_allowed = True
    samplingfeatureuuid: uuid.UUID
    samplingfeaturetypecv: constr(max_length=255)  # type: ignore
    samplingfeaturecode: constr(max_length=50)  # type: ignore
    samplingfeaturename: constr(max_length=255) = None  # type: ignore
    samplingfeaturedescription: constr(max_length=5000) = None  # type: ignore
    samplingfeaturegeotypecv: constr(max_length=255) = None  # type: ignore
    featuregeometrywkt: constr(max_length=8000) = None  # type: ignore
    elevation_m: Optional[float]
    elevationdatumcv: constr(max_length=255) = None  # type: ignore

    @validator('featuregeometrywkt')
    def username_alphanumeric(cls, wkt):
        new_shape = shapely.wkt.loads(wkt)
        assert new_shape.is_valid
        return wkt


class SamplingFeatures(SamplingFeaturesCreate):
    samplingfeatureid: int


class SpatialReferencesCreate(BaseModel):
    srscode: constr(max_length=50) = None  # type: ignore
    srsname: constr(max_length=255)  # type: ignore
    srsdescription: constr(max_length=5000) = None  # type: ignore
    srslink: constr(max_length=255) = None  # type: ignore


class SpatialReferences(SpatialReferencesCreate):
    spatialreferenceid: int


class Sites(BaseModel):
    samplingfeatureid: int
    sitetypecv: constr(max_length=255)  # type: ignore
    latitude: float
    longitude: float
    spatialreferenceid: int


class ProcessingLevelsCreate(BaseModel):
    processinglevelcode: constr(max_length=50)  # type: ignore
    definition: constr(max_length=5000) = None  # type: ignore
    explanation: constr(max_length=5000) = None  # type: ignore


class ProcessingLevels(ProcessingLevelsCreate):
    processinglevelid: int


class UnitsCreate(BaseModel):
    unitstypecv: constr(max_length=255)  # type: ignore
    unitsabbreviation: constr(max_length=50)  # type: ignore
    unitsname: constr(max_length=255)  # type: ignore
    unitslink: constr(max_length=255) = None  # type: ignore


class Units(UnitsCreate):
    unitsid: int


class VariablesCreate(BaseModel):
    variabletypecv: constr(max_length=255)  # type: ignore
    variablecode: constr(max_length=50)  # type: ignore
    variablenamecv: constr(max_length=255)  # type: ignore
    variabledefinition: constr(max_length=5000) = None  # type: ignore
    speciationcv: constr(max_length=255) = None  # type: ignore
    nodatavalue: float


class Variables(VariablesCreate):
    variableid: int


class DataQualityCreate(BaseModel):
    dataqualitytypecv: constr(max_length=255)  # type: ignore
    dataqualitycode: constr(max_length=255)  # type: ignore
    dataqualityvalue: Optional[float]
    dataqualityvalueunitsid: Optional[int]
    dataqualitydescription: constr(max_length=5000) = None  # type: ignore
    dataqualitylink: constr(max_length=255) = None  # type: ignore


class DataQuality(DataQualityCreate):
    dataqualityid: int


class ResultsDataQualityCreate(BaseModel):
    resultid: int
    dataqualityid: int


class ResultsDataQuality(ResultsDataQualityCreate):
    bridgeid: int


class FeatureActionsCreate(BaseModel):
    samplingfeatureid: int
    actionid: int


class FeatureActions(FeatureActionsCreate):
    featureactionid: int


class ResultsCreate(FeatureActionsCreate):
    dataqualityid: Optional[int]
    resultuuid: uuid.UUID
    resulttypecv: constr(max_length=255)  # type: ignore
    variableid: int
    unitsid: int
    taxonomicclassifierid: Optional[int] = None
    processinglevelid: int
    resultdatetime: Optional[datetime.datetime] = None
    resultdatetimeutcoffset: Optional[int] = None
    validdatetime: Optional[datetime.datetime] = None
    validdatetimeutcoffset: Optional[int] = None
    statuscv: constr(max_length=255) = None  # type: ignore
    sampledmediumcv: constr(max_length=5000) = None  # type: ignore
    valuecount: int


class Results(ResultsCreate):
    resultid: int
