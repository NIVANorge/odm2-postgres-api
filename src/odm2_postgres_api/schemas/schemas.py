from typing import Optional
import datetime

from pydantic import BaseModel, constr

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


class ActionsBy(ActionsByFields):
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
