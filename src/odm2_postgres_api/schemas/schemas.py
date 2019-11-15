from typing import Optional

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
