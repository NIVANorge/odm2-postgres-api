import asyncpg
import shapely.wkt

from odm2_postgres_api.schemas import schemas
from odm2_postgres_api.utils import shapely_postgres_adapter


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
    # Dict allows overwriting of key while pydantic schema does not, identical action_id exists in both return rows
    return schemas.Action(**{**action_row, **dict(action_by_row)})


async def create_sampling_feature(conn: asyncpg.connection, sampling_feature: schemas.SamplingFeaturesCreate):
    await shapely_postgres_adapter.set_shapely_adapter(conn)
    row = await conn.fetchrow(
        "INSERT INTO samplingfeatures (samplingfeatureuuid, samplingfeaturetypecv, samplingfeaturecode, "
        "samplingfeaturename, samplingfeaturedescription, samplingfeaturegeotypecv, featuregeometry, "
        "featuregeometrywkt, elevation_m, elevationdatumcv) "
        "VALUES ($1, $2, $3, $4, $5, $6, "
        "ST_SetSRID($7::geometry, 4326), $8, $9, $10) returning *",
        sampling_feature.samplingfeatureuuid, sampling_feature.samplingfeaturetypecv,
        sampling_feature.samplingfeaturecode, sampling_feature.samplingfeaturename,
        sampling_feature.samplingfeaturedescription, sampling_feature.samplingfeaturegeotypecv,
        shapely.wkt.loads(sampling_feature.featuregeometrywkt), sampling_feature.featuregeometrywkt,
        sampling_feature.elevation_m, sampling_feature.elevationdatumcv
    )
    return schemas.SamplingFeatures(**row)


async def create_processing_level(conn: asyncpg.connection, processing_level: schemas.ProcessingLevelsCreate):
    row = await conn.fetchrow(
        "INSERT INTO processinglevels (processinglevelcode, definition, explanation) "
        "VALUES ($1, $2, $3) returning *",
        processing_level.processinglevelcode, processing_level.definition, processing_level.explanation)
    return schemas.ProcessingLevels(**row)


async def create_unit(conn: asyncpg.connection, unit: schemas.UnitsCreate):
    row = await conn.fetchrow(
        "INSERT INTO units (unitstypecv, unitsabbreviation, unitsname, unitslink) "
        "VALUES ($1, $2, $3, $4) returning *",
        unit.unitstypecv, unit.unitsabbreviation, unit.unitsname, unit.unitslink)
    return schemas.Units(**row)


async def create_variable(conn: asyncpg.connection, variable: schemas.VariablesCreate):
    row = await conn.fetchrow(
        "INSERT INTO variables (variabletypecv, variablecode, variablenamecv, variabledefinition, "
        "speciationcv, nodatavalue) VALUES ($1, $2, $3, $4, $5, $6) returning *",
        variable.variabletypecv, variable.variablecode, variable.variablenamecv, variable.variabledefinition,
        variable.speciationcv, variable.nodatavalue)
    return schemas.Variables(**row)


async def create_feature_action(conn: asyncpg.connection, feature_action: schemas.FeatureActionsCreate):
    row = await conn.fetchrow(
        "INSERT INTO featureactions (samplingfeatureid, actionid) "
        "VALUES ($1, $2) "
        "ON CONFLICT (samplingfeatureid, actionid) DO UPDATE SET actionid = EXCLUDED.actionid returning *",
        feature_action.samplingfeatureid, feature_action.actionid)
    return schemas.FeatureActions(**row)


async def create_result(conn: asyncpg.connection, result: schemas.ResultsCreate):
    async with conn.transaction():
        feature_action_row = await create_feature_action(conn, schemas.FeatureActionsCreate(
            samplingfeatureid=result.samplingfeatureid, actionid=result.actionid))
        result_row = await conn.fetchrow(
            "INSERT INTO results (resultuuid, featureactionid, resulttypecv, variableid, unitsid,"
            "taxonomicclassifierid, processinglevelid, resultdatetime, resultdatetimeutcoffset, validdatetime,"
            "validdatetimeutcoffset, statuscv, sampledmediumcv, valuecount) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14) returning *",
            result.resultuuid, feature_action_row.featureactionid, result.resulttypecv, result.variableid,
            result.unitsid, result.taxonomicclassifierid, result.processinglevelid, result.resultdatetime,
            result.resultdatetimeutcoffset, result.validdatetime, result.validdatetimeutcoffset, result.statuscv,
            result.sampledmediumcv, result.valuecount)
    # Dict allows overwriting of key while pydantic schema does not, featureactionid exists in both return rows
    return schemas.Results(**{**result_row, **dict(feature_action_row)})
