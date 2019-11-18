import asyncpg

from odm2_postgres_api.schemas import schemas


async def create_person(conn: asyncpg.connection, user: schemas.PeopleCreate):
    row = await conn.fetchrow(
        "INSERT INTO people (personfirstname, personmiddlename, personlastname) VALUES ($1, $2, $3) returning *",
        user.personfirstname, user.personmiddlename, user.personlastname)
    return schemas.People(**row)


async def create_organization(conn: asyncpg.connection, organization: schemas.OrganizationsCreate):
    row = await conn.fetchrow(
        "INSERT INTO organizations (organizationtypecv, organizationcode, organizationname, organizationdescription, "
        "organizationlink, parentorganizationid) VALUES ($1, $2, $3, $4, $5, $6) returning *",
        organization.organizationtypecv, organization.organizationcode, organization.organizationname,
        organization.organizationdescription, organization.organizationlink, organization.parentorganizationid)
    return schemas.Organizations(**row)


async def create_affiliation(conn: asyncpg.connection, affiliation: schemas.AffiliationsCreate):
    row = await conn.fetchrow(
        "INSERT INTO affiliations (personid, organizationid, isprimaryorganizationcontact, affiliationstartdate, "
        "affiliationenddate, primaryphone, primaryemail, primaryaddress, personlink) "
        "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) returning *",
        affiliation.personid, affiliation.organizationid, affiliation.isprimaryorganizationcontact,
        affiliation.affiliationstartdate, affiliation.affiliationenddate, affiliation.primaryphone,
        affiliation.primaryemail, affiliation.primaryaddress, affiliation.personlink)
    return schemas.Affiliations(**row)


async def create_method(conn: asyncpg.connection, method: schemas.MethodsCreate):
    row = await conn.fetchrow(
        "INSERT INTO methods (methodtypecv, methodcode, methodname, methoddescription, methodlink, organizationid) "
        "VALUES ($1, $2, $3, $4, $5, $6) returning *",
        method.methodtypecv, method.methodcode, method.methodname, method.methoddescription,
        method.methodlink, method.organizationid)
    return schemas.Methods(**row)


async def create_action_by(conn: asyncpg.connection, action_by: schemas.ActionsByCreate):
    row = await conn.fetchrow(
        "INSERT INTO actionby (actionid, affiliationid, isactionlead, roledescription) "
        "VALUES ($1, $2, $3, $4) returning *",
        action_by.actionid, action_by.affiliationid, action_by.isactionlead, action_by.roledescription)
    return schemas.ActionsBy(**row)


async def do_action(conn: asyncpg.connection, action: schemas.ActionsCreate):
    async with conn.transaction():
        action_row = await conn.fetchrow(
            "INSERT INTO actions (actiontypecv, methodid, begindatetime, begindatetimeutcoffset,  enddatetime, "
            "enddatetimeutcoffset, actiondescription, actionfilelink) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7, $8) returning *",
            action.actiontypecv, action.methodid, action.begindatetime, action.begindatetimeutcoffset,
            action.enddatetime, action.enddatetimeutcoffset, action.actiondescription, action.actionfilelink)
        action_by = schemas.ActionsByCreate(actionid=action_row['actionid'], affiliationid=action.affiliationid,
                                            isactionlead=action.isactionlead, roledescription=action.roledescription)
        action_by_row = await create_action_by(conn, action_by)
    return schemas.Action(**action_row, **dict(action_by_row))
