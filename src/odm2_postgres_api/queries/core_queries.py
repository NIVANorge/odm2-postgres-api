import logging

import asyncpg

from odm2_postgres_api.schemas import schemas


async def create_person(conn: asyncpg.connection, user: schemas.PeopleCreate):
    row = await conn.fetchrow("INSERT INTO people (personfirstname, personmiddlename, personlastname) "
                              "VALUES ($1, $2, $3) returning *",
                              user.personfirstname, user.personmiddlename, user.personlastname)
    return schemas.People(**row)


async def create_organization(conn: asyncpg.connection, organization: schemas.OrganizationsCreate):
    row = await conn.fetchrow("INSERT INTO organizations (organizationtypecv, organizationcode, organizationname,"
                              "organizationdescription, organizationlink, parentorganizationid) "
                              "VALUES ($1, $2, $3, $4, $5, $6) returning *",
                              organization.organizationtypecv, organization.organizationcode,
                              organization.organizationname, organization.organizationdescription,
                              organization.organizationlink, organization.parentorganizationid)
    return schemas.Organizations(**row)
