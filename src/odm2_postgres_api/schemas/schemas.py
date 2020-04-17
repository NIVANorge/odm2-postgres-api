import uuid
import datetime
from typing import Optional, List, Tuple

import shapely.wkt
from pydantic import BaseModel, BaseConfig, constr, validator

from odm2_postgres_api.queries.controlled_vocabulary_queries import CONTROLLED_VOCABULARY_TABLE_NAMES

cv_checker = constr(regex=f'({"|".join([f"^{cv}$" for cv in CONTROLLED_VOCABULARY_TABLE_NAMES])})')


class ControlledVocabulary(BaseModel):
    term: constr(max_length=255)  # type: ignore
    name: constr(max_length=255) = None  # type: ignore
    definition: constr(max_length=255) = None  # type: ignore
    category: constr(max_length=255) = None  # type: ignore
    # sourcevocabularyuri: constr(max_length=255) = None  # type: ignore
    controlled_vocabulary_table_name: cv_checker  # type: ignore


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


class DatasetCreate(BaseModel):
    datasetuuid: uuid.UUID
    datasettypecv: constr(max_length=255)
    datasetcode: constr(max_length=50)
    datasettitle: constr(max_length=255)
    datasetabstract: constr(max_length=5000)


class Dataset(DatasetCreate):
    datasetid: int


class EquipmentModelCreate(BaseModel):
    modelmanufacturerid: int
    modelpartnumber: constr(max_length=50) = None  # type: ignore
    modelname: constr(max_length=255)  # type: ignore
    modeldescription: constr(max_length=5000) = None  # type: ignore
    isinstrument: bool
    modelspecificationsfilelink: constr(max_length=255) = None  # type: ignore
    modellink: constr(max_length=255) = None  # type: ignore


class EquipmentModel(EquipmentModelCreate):
    equipmentmodelid: int


class InstrumentOutputVariablesCreate(BaseModel):
    modelid: int
    variableid: int
    instrumentmethodid: int
    instrumentresolution: constr(max_length=255) = None  # type: ignore
    instrumentaccuracy: constr(max_length=255) = None  # type: ignore
    instrumentrawoutputunitsid: int


class InstrumentOutputVariables(InstrumentOutputVariablesCreate):
    instrumentoutputvariableid: int


class EquipmentCreate(BaseModel):
    equipmentcode: constr(max_length=50)  # type: ignore
    equipmentname: constr(max_length=255)  # type: ignore
    equipmenttypecv: constr(max_length=255)  # type: ignore
    equipmentmodelid: int
    equipmentserialnumber: constr(max_length=50)  # type: ignore
    equipmentownerid: int
    equipmentvendorid: int
    equipmentpurchasedate: datetime.datetime
    equipmentpurchaseordernumber: constr(max_length=50) = None  # type: ignore
    equipmentdescription: constr(max_length=5000) = None  # type: ignore
    equipmentdocumentationlink: constr(max_length=255) = None  # type: ignore


class Equipment(EquipmentCreate):
    equipmentid: int


class EquipmentUsedCreate(BaseModel):
    actionid: int
    equipmentid: int


class EquipmentUsed(EquipmentUsedCreate):
    bridgeid: int


class MethodsCreate(BaseModel):
    methodtypecv: constr(max_length=255)  # type: ignore
    methodcode: constr(max_length=50)  # type: ignore
    methodname: constr(max_length=255)  # type: ignore
    methoddescription: constr(max_length=5000) = None  # type: ignore
    methodlink: constr(max_length=255) = None  # type: ignore
    organizationid: Optional[int] = None


class Methods(MethodsCreate):
    methodid: int


class RelatedActionCreate(BaseModel):
    actionid: int
    relationshiptypecv: constr(max_length=255)  # type: ignore
    relatedactionid: int


class RelatedAction(RelatedActionCreate):
    relationid: int


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
    equipmentids: List[int] = []
    relatedactions: List[Tuple[int, str]] = []


class Action(ActionsCreate):
    actionid: int


class SamplingFeaturesCreate(BaseModel):
    samplingfeatureuuid: uuid.UUID
    samplingfeaturetypecv: constr(max_length=255)  # type: ignore
    samplingfeaturecode: constr(max_length=50)  # type: ignore
    samplingfeaturename: constr(max_length=255) = None  # type: ignore
    samplingfeaturedescription: constr(max_length=5000) = None  # type: ignore
    samplingfeaturegeotypecv: constr(max_length=255) = None  # type: ignore
    featuregeometrywkt: constr(max_length=8000) = None  # type: ignore
    elevation_m: Optional[float]
    elevationdatumcv: constr(max_length=255) = None  # type: ignore
    relatedsamplingfeatures: List[Tuple[int, str]] = []

    @validator('featuregeometrywkt')
    def featuregeometrywkt_validator(cls, wkt):
        if wkt:
            new_shape = shapely.wkt.loads(wkt)
            if not new_shape.is_valid:
                raise ValueError('well known text is not valid!')
        return wkt


class SamplingFeatures(SamplingFeaturesCreate):
    samplingfeatureid: int


class RelatedSamplingFeatureCreate(BaseModel):
    samplingfeatureid: int
    relationshiptypecv: constr(max_length=255)  # type: ignore
    relatedfeatureid: int
    spatialoffsetid: Optional[int]


class RelatedSamplingFeature(RelatedSamplingFeatureCreate):
    relationid: int


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


class DataQualityCreate(BaseModel):
    dataqualitytypecv: constr(max_length=255)  # type: ignore
    dataqualitycode: constr(max_length=255)  # type: ignore
    dataqualityvalue: Optional[float]
    dataqualityvalueunitsid: Optional[int]
    dataqualitydescription: constr(max_length=5000) = None  # type: ignore
    dataqualitylink: constr(max_length=255) = None  # type: ignore


class DataQuality(DataQualityCreate):
    dataqualityid: int


class ResultsDatasetCreate(BaseModel):
    dataset_uuids: List[uuid.UUID]
    results_uuid: uuid.UUID


class ResultsDataset(BaseModel):
    bridgeid: int
    datasetid: int
    resultsid: int


class ResultsDataQualityCreate(BaseModel):
    resultid: int
    dataqualitycode: str


class ResultsDataQuality(ResultsDataQualityCreate):
    bridgeid: int


class TaxonomicClassifierCreate(BaseModel):
    taxonomicclassifiertypecv: constr(max_length=255)  # type: ignore
    taxonomicclassifiername: constr(max_length=255)  # type: ignore
    taxonomicclassifiercommonname: constr(max_length=255) = None  # type: ignore
    taxonomicclassifierdescription: constr(max_length=5000) = None  # type: ignore
    parenttaxonomicclassifierid: Optional[int]


class TaxonomicClassifier(TaxonomicClassifierCreate):
    taxonomicclassifierid: int


class FeatureActionsCreate(BaseModel):
    samplingfeatureuuid: uuid.UUID
    actionid: int


class FeatureActions(FeatureActionsCreate):
    featureactionid: int


class ResultsCreate(FeatureActionsCreate):
    dataqualitycodes: List[str]
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
    datasetuuids: List[uuid.UUID]


class Results(ResultsCreate):
    resultid: int


class TrackResultsFields(BaseModel):
    resultid: int
    samplingfeatureid: int
    intendedtimespacing: Optional[float]
    intendedtimespacingunitsid: Optional[int]
    aggregationstatisticcv: constr(max_length=255)  # type: ignore


class TrackResultsCreate(TrackResultsFields):
    track_result_values: List[Tuple[datetime.datetime, float, str]]
    track_result_locations: List[Tuple[datetime.datetime, float, float, str]]


class TrackResultsReport(TrackResultsFields):
    inserted_track_result_values: int
    inserted_track_result_locations: int


class ResultSharedBase(BaseModel):
    resultid: int
    xlocation: Optional[float]
    xlocationunitsid: Optional[int]
    ylocation: Optional[float]
    ylocationunitsid: Optional[int]
    zlocation: Optional[float]
    zlocationunitsid: Optional[int]
    spatialreferenceid: Optional[int]


class CategoricalResultsCreate(ResultSharedBase):
    qualitycodecv: constr(max_length=255)  # type: ignore
    datavalue: constr(max_length=255)  # type: ignore
    valuedatetime: datetime.datetime
    valuedatetimeutcoffset: int


class CategoricalResults(CategoricalResultsCreate):
    valueid: int


class MeasurementResultsCreate(ResultSharedBase):
    censorcodecv: constr(max_length=255)  # type: ignore
    qualitycodecv: constr(max_length=255)  # type: ignore
    aggregationstatisticcv: constr(max_length=255)  # type: ignore
    timeaggregationinterval: float
    timeaggregationintervalunitsid: int
    datavalue: float
    valuedatetime: datetime.datetime
    valuedatetimeutcoffset: int


class MeasurementResults(MeasurementResultsCreate):
    valueid: int
