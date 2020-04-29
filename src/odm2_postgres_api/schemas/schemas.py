import uuid
import datetime as dt
from typing import Optional, List, Tuple

import shapely.wkt
from pydantic import BaseModel, constr, validator

from odm2_postgres_api.queries.controlled_vocabulary_queries import CONTROLLED_VOCABULARY_TABLE_NAMES


def create_obligatory_date_time_checker(datetime_name, offset_name):
    def check_utc_offset(date_time: dt.datetime, values):
        if date_time.tzinfo:
            if values[offset_name] != date_time.utcoffset().seconds / 3600:
                raise ValueError(f"conflicting utcoffset on '{datetime_name}.utcoffset().seconds/3600="
                                 f"{date_time.utcoffset().seconds / 3600}' and "
                                 f"'values[{offset_name}]={values[offset_name]}'")
        else:
            if values[offset_name] != 0:
                raise ValueError(f"'{offset_name}' should be 0 when no timezone on '{datetime_name}' is supplied")
        return date_time.replace(tzinfo=None)

    return check_utc_offset


def create_optional_date_time_checker(datetime_name, offset_name):
    obligatory_check = create_obligatory_date_time_checker(datetime_name, offset_name)

    def check_utc_offset(date_time: dt.datetime, values):
        if not date_time:
            return date_time
        if values[offset_name] is None:
            raise ValueError(f"Missing '{offset_name}', When '{datetime_name}' is "
                             f"supplied you must supply '{offset_name}'")
        return obligatory_check(date_time, values)

    return check_utc_offset


cv_checker = constr(regex=f'({"|".join([f"^{cv}$" for cv in CONTROLLED_VOCABULARY_TABLE_NAMES])})')
begindatetime_checker = create_obligatory_date_time_checker('begindatetime', 'begindatetimeutcoffset')
enddatetime_checker = create_optional_date_time_checker('enddatetime', 'enddatetimeutcoffset')
resultdatetime_checker = create_optional_date_time_checker('resultdatetime', 'resultdatetimeutcoffset')
validdatetime_checker = create_optional_date_time_checker('validdatetime', 'validdatetimeutcoffset')
valuedatetime_checker = create_obligatory_date_time_checker('valuedatetime', 'valuedatetimeutcoffset')


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


class PersonExternalIdentifiers(BaseModel):
    bridgeid: int
    personid: int
    externalidentifiersystemid: int
    personexternalidentifier: constr(max_length=255)  # type: ignore
    personexternalidentifieruri: Optional[str]


class OrganizationsCreate(BaseModel):
    organizationtypecv: constr(max_length=255)  # type: ignore
    organizationcode: constr(max_length=50)  # type: ignore
    organizationname: constr(max_length=255)  # type: ignore
    organizationdescription: constr(max_length=5000) = None  # type: ignore
    organizationlink: constr(max_length=255) = None  # type: ignore
    parentorganizationid: Optional[int] = None


class Organizations(OrganizationsCreate):
    organizationid: int


class ExternalIdentifierSystemsCreate(BaseModel):
    externalidentifiersystemname: constr(max_length=255)  # type: ignore
    identifiersystemorganizationid: int
    externalidentifiersystemdescription: constr(max_length=5000) = None  # type: ignore
    externalidentifiersystemurl: constr(max_length=255) = None  # type: ignore


class ExternalIdentifierSystems(ExternalIdentifierSystemsCreate):
    externalidentifiersystemid: int


class AffiliationsCreate(BaseModel):
    personid: int
    organizationid: Optional[int] = None
    isprimaryorganizationcontact: Optional[bool] = None
    affiliationstartdate: dt.date
    affiliationenddate: Optional[dt.date] = None
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
    equipmentpurchasedate: dt.datetime
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


class DirectivesCreate(BaseModel):
    directivetypecv: constr(max_length=255)  # type: ignore
    directivedescription: constr(max_length=5000) = None  # type: ignore


class Directive(DirectivesCreate):
    directiveid: int


class ActionDirectivesCreate(BaseModel):
    actionid: int
    directiveid: int


class ActionDirective(ActionDirectivesCreate):
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


class BeginDateTimeBase(BaseModel):
    begindatetimeutcoffset: int
    begindatetime: dt.datetime

    @validator('begindatetime')
    def check_utc_offset(cls, begindatetime: dt.datetime, values):
        return begindatetime_checker(begindatetime, values)


class EndDateTimeBase(BaseModel):
    enddatetimeutcoffset: Optional[int] = None
    enddatetime: Optional[dt.datetime] = None

    @validator('enddatetime')
    def check_utc_offset(cls, enddatetime: dt.datetime, values):
        return enddatetime_checker(enddatetime, values)


class ActionsCreate(ActionsByFields, BeginDateTimeBase, EndDateTimeBase):
    actiontypecv: constr(max_length=255)  # type: ignore
    methodid: int
    actiondescription: constr(max_length=5000) = None  # type: ignore
    actionfilelink: constr(max_length=255) = None  # type: ignore
    equipmentids: List[int] = []
    directiveids: List[int] = []
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
    resultdatetimeutcoffset: Optional[int] = None
    resultdatetime: Optional[dt.datetime] = None
    validdatetimeutcoffset: Optional[int] = None
    validdatetime: Optional[dt.datetime] = None
    statuscv: constr(max_length=255) = None  # type: ignore
    sampledmediumcv: constr(max_length=5000) = None  # type: ignore
    valuecount: int

    @validator('resultdatetime')
    def check_resultdatetime_utc_offset(cls, enddatetime: dt.datetime, values):
        return resultdatetime_checker(enddatetime, values)

    @validator('validdatetime')
    def check_validdatetime_utc_offset(cls, enddatetime: dt.datetime, values):
        return validdatetime_checker(enddatetime, values)


class Results(ResultsCreate):
    resultid: int


class TrackResultsFields(BaseModel):
    resultid: int
    samplingfeatureid: int
    intendedtimespacing: Optional[float]
    intendedtimespacingunitsid: Optional[int]
    aggregationstatisticcv: constr(max_length=255)  # type: ignore


class TrackResultsCreate(TrackResultsFields):
    track_result_values: List[Tuple[dt.datetime, float, str]]
    track_result_locations: List[Tuple[dt.datetime, float, float, str]]


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
    valuedatetimeutcoffset: int
    valuedatetime: dt.datetime

    @validator('valuedatetime')
    def check_utc_offset(cls, enddatetime: dt.datetime, values):
        return valuedatetime_checker(enddatetime, values)


class CategoricalResultsCreate(ResultSharedBase):
    qualitycodecv: constr(max_length=255)  # type: ignore
    datavalue: constr(max_length=255)  # type: ignore


class CategoricalResults(CategoricalResultsCreate):
    valueid: int


class MeasurementResultsCreate(ResultSharedBase):
    censorcodecv: constr(max_length=255)  # type: ignore
    qualitycodecv: constr(max_length=255)  # type: ignore
    aggregationstatisticcv: constr(max_length=255)  # type: ignore
    timeaggregationinterval: float
    timeaggregationintervalunitsid: int
    datavalue: float


class MeasurementResults(MeasurementResultsCreate):
    valueid: int


class BegroingResultCreate(BaseModel):
    form: dict


class BegroingResult(BegroingResultCreate):
    personid: int


if __name__ == '__main__':
    BeginDateTimeBase(begindatetime=dt.datetime.fromisoformat('2019-08-27T22:00:00+01:00'), begindatetimeutcoffset=1)
    # BeginDateTimeBase(begindatetime=dt.datetime.fromisoformat('2019-08-27T22:00:00+01:00'))  # Error!
    BeginDateTimeBase(begindatetime=dt.datetime.fromisoformat('2019-08-27T22:00:00+00:00'), begindatetimeutcoffset=0)
    # BeginDateTimeBase(begindatetime=dt.datetime.fromisoformat('2019-08-27T22:00:00+00:00'), begindatetimeutcoffset=1)
    BeginDateTimeBase(begindatetime=dt.datetime.fromisoformat('2019-08-27T22:00:00'), begindatetimeutcoffset=0)
    # BeginDateTimeBase(begindatetime=dt.datetime.fromisoformat('2019-08-27T22:00:00'), begindatetimeutcoffset=1)

    EndDateTimeBase(enddatetime=None)
    EndDateTimeBase(enddatetime=None, enddatetimeutcoffset=None)
    # EndDateTimeBase(enddatetime=dt.datetime.fromisoformat('2019-08-27T22:00:00'))  # Error!
    # EndDateTimeBase(enddatetime=dt.datetime.fromisoformat('2019-08-27T22:00:00+00:00'))  # Error!
    # EndDateTimeBase(enddatetime=dt.datetime.fromisoformat('2019-08-27T22:00:00+01:00'), enddatetimeutcoffset=2) #Err
    EndDateTimeBase(enddatetime=dt.datetime.fromisoformat('2019-08-27T22:00:00+00:00'), enddatetimeutcoffset=0)
    EndDateTimeBase(enddatetime=dt.datetime.fromisoformat('2019-08-27T22:00:00+01:00'), enddatetimeutcoffset=1)
