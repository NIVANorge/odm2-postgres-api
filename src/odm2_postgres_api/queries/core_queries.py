import asyncpg
import shapely.wkt
import logging

from odm2_postgres_api.schemas import schemas
from odm2_postgres_api.utils import shapely_postgres_adapter
from odm2_postgres_api.queries.controlled_vocabulary_queries import CONTROLLED_VOCABULARY_TABLE_NAMES


async def create_new_controlled_vocabulary_item(conn: asyncpg.connection,
                                                controlled_vocabulary: schemas.ControlledVocabulary):
    table_name = controlled_vocabulary.controlled_vocabulary_table_name
    # Check against hardcoded table names otherwise this could be an SQL injection
    if table_name not in CONTROLLED_VOCABULARY_TABLE_NAMES:
        raise RuntimeError(f"table_name: '{table_name}' is invalid")
    sql_statement = f"INSERT INTO {table_name} (term, name, definition, category) " \
                    f"VALUES ($1, $2, $3, $4) returning *"
    row = await conn.fetchrow(sql_statement, controlled_vocabulary.term, controlled_vocabulary.name,
                              controlled_vocabulary.definition, controlled_vocabulary.category)
    return schemas.ControlledVocabulary(**{**dict(controlled_vocabulary), **row})


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


async def create_equipment_model(conn: asyncpg.connection, equipment_model: schemas.EquipmentModelCreate):
    row = await conn.fetchrow(
        "INSERT INTO equipmentmodels (modelmanufacturerid, modelpartnumber, modelname, modeldescription, isinstrument,"
        " modelspecificationsfilelink, modellink) VALUES ($1, $2, $3, $4, $5, $6, $7) returning *",
        equipment_model.modelmanufacturerid, equipment_model.modelpartnumber, equipment_model.modelname,
        equipment_model.modeldescription, equipment_model.isinstrument, equipment_model.modelspecificationsfilelink,
        equipment_model.modellink)
    return schemas.Variables(**row)


async def create_instrument_output_variable(conn: asyncpg.connection,
                                            instrument_output_variable: schemas.InstrumentOutputVariablesCreate):
    row = await conn.fetchrow(
        "INSERT INTO instrumentoutputvariables (modelid, variableid, instrumentmethodid, instrumentresolution,"
        "instrumentaccuracy, instrumentrawoutputunitsid) VALUES ($1, $2, $3, $4, $5, $6) returning *",
        instrument_output_variable.modelid, instrument_output_variable.variableid,
        instrument_output_variable.instrumentmethodid, instrument_output_variable.instrumentresolution,
        instrument_output_variable.instrumentaccuracy, instrument_output_variable.instrumentrawoutputunitsid)
    return schemas.Variables(**row)


async def create_equipment(conn: asyncpg.connection, equipment: schemas.EquipmentCreate):
    row = await conn.fetchrow(
        "INSERT INTO equipment (equipmentcode, equipmentname, equipmenttypecv, equipmentmodelid,"
        "equipmentserialnumber, equipmentownerid, equipmentvendorid, equipmentpurchasedate,"
        "equipmentpurchaseordernumber, equipmentdescription, equipmentdocumentationlink) "
        "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) returning *",
        equipment.equipmentcode, equipment.equipmentname, equipment.equipmenttypecv, equipment.equipmentmodelid,
        equipment.equipmentserialnumber, equipment.equipmentownerid, equipment.equipmentvendorid,
        equipment.equipmentpurchasedate, equipment.equipmentpurchaseordernumber, equipment.equipmentdescription,
        equipment.equipmentdocumentationlink)
    return schemas.Equipment(**row)


async def create_equipment_used(conn: asyncpg.connection, equipmentused: schemas.EquipmentUsedCreate):
    row = await conn.fetchrow(
        "INSERT INTO equipmentused (actionid, equipmentid) VALUES ($1, $2) returning *",
        equipmentused.actionid, equipmentused.equipmentid)
    return schemas.EquipmentUsed(**row)


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


async def create_related_action(conn: asyncpg.connection, related_action: schemas.RelatedActionCreate):
    row = await conn.fetchrow(
        "INSERT INTO niva_odm2.odm2.relatedactions (actionid, relationshiptypecv, relatedactionid) "
        "VALUES ($1, $2, $3) returning *",
        related_action.actionid, related_action.relationshiptypecv, related_action.relatedactionid)
    return schemas.RelatedAction(**row)


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
        for equipmentid in action.equipmentids:
            await create_equipment_used(conn, schemas.EquipmentUsedCreate(
                actionid=action_row['actionid'], equipmentid=equipmentid))
        for action_id, relation_ship_type in action.relatedactions:
            await create_related_action(conn, schemas.RelatedActionCreate(
                actionid=action_row['actionid'], relationshiptypecv=relation_ship_type, relatedactionid=action_id))
    # Dict allows overwriting of key while pydantic schema does not, identical action_id exists in both return rows
    return schemas.Action(equipmentids=action.equipmentids, **{**action_row, **dict(action_by_row)})


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


async def create_spatial_reference(conn: asyncpg.connection, spatial_reference: schemas.SpatialReferencesCreate):
    row = await conn.fetchrow(
        "INSERT INTO spatialreferences (srscode, srsname, srsdescription, srslink) "
        "VALUES ($1, $2, $3, $4) returning *",
        spatial_reference.srscode, spatial_reference.srsname,
        spatial_reference.srsdescription, spatial_reference.srslink)
    return schemas.SpatialReferences(**row)


async def create_site(conn: asyncpg.connection, site: schemas.Sites):
    row = await conn.fetchrow(
        "INSERT INTO sites (samplingfeatureid, sitetypecv, latitude, longitude, spatialreferenceid) "
        "VALUES ($1, $2, $3, $4, $5) returning *",
        site.samplingfeatureid, site.sitetypecv, site.latitude, site.longitude, site.spatialreferenceid)
    return schemas.Sites(**row)


async def create_data_quality(conn: asyncpg.connection, data_quality: schemas.DataQualityCreate):
    row = await conn.fetchrow(
        "INSERT INTO dataquality (dataqualitytypecv, dataqualitycode, dataqualityvalue, dataqualityvalueunitsid,"
        "dataqualitydescription, dataqualitylink) VALUES ($1, $2, $3, $4, $5, $6) returning *",
        data_quality.dataqualitytypecv, data_quality.dataqualitycode, data_quality.dataqualityvalue,
        data_quality.dataqualityvalueunitsid, data_quality.dataqualitydescription, data_quality.dataqualitylink)
    return schemas.DataQuality(**row)


async def create_result_data_quality(conn: asyncpg.connection, result_data_quality: schemas.ResultsDataQualityCreate):
    row = await conn.fetchrow(
        "INSERT INTO resultsdataquality (resultid, dataqualityid) VALUES ($1, $2) "
        "ON CONFLICT (resultid, dataqualityid) DO UPDATE SET resultid = EXCLUDED.resultid returning *",
        result_data_quality.resultid, result_data_quality.dataqualityid)
    return schemas.ResultsDataQuality(**row)


async def create_feature_action(conn: asyncpg.connection, feature_action: schemas.FeatureActionsCreate):
    row = await conn.fetchrow(
        "INSERT INTO featureactions (samplingfeatureid, actionid) "
        "VALUES ((SELECT samplingfeatureid FROM samplingfeatures where samplingfeatureuuid = $1), $2) "
        "ON CONFLICT (samplingfeatureid, actionid) DO UPDATE SET actionid = EXCLUDED.actionid returning *",
        feature_action.samplingfeatureuuid, feature_action.actionid)
    return schemas.FeatureActions(samplingfeatureuuid=feature_action.samplingfeatureuuid, **row)


async def create_result(conn: asyncpg.connection, result: schemas.ResultsCreate):
    async with conn.transaction():
        feature_action_row = await create_feature_action(conn, schemas.FeatureActionsCreate(
            samplingfeatureuuid=result.samplingfeatureuuid, actionid=result.actionid))
        result_row = await conn.fetchrow(
            "INSERT INTO results (resultuuid, featureactionid, resulttypecv, variableid, unitsid,"
            "taxonomicclassifierid, processinglevelid, resultdatetime, resultdatetimeutcoffset, validdatetime,"
            "validdatetimeutcoffset, statuscv, sampledmediumcv, valuecount) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14) returning *",
            result.resultuuid, feature_action_row.featureactionid, result.resulttypecv, result.variableid,
            result.unitsid, result.taxonomicclassifierid, result.processinglevelid, result.resultdatetime,
            result.resultdatetimeutcoffset, result.validdatetime, result.validdatetimeutcoffset, result.statuscv,
            result.sampledmediumcv, result.valuecount)
        for data_quality_id in result.dataqualityids:
            await create_result_data_quality(conn, schemas.ResultsDataQualityCreate(
                resultid=result_row['resultid'], dataqualityid=data_quality_id))
    # Dict allows overwriting of key while pydantic schema does not, featureactionid exists in both return rows
    return schemas.Results(dataqualityids=result.dataqualityids, **{**result_row, **dict(feature_action_row)})


async def upsert_track_result(conn: asyncpg.connection, track_result: schemas.TrackResultsCreate):
    await shapely_postgres_adapter.set_shapely_adapter(conn)
    async with conn.transaction():
        row = await conn.fetchrow("SELECT samplingfeatureid FROM featureactions WHERE featureactionid = "
                                  "(SELECT featureactionid FROM results WHERE resultid = $1)", track_result.resultid)
        if row["samplingfeatureid"] != track_result.samplingfeatureid:
            raise ValueError('THIS IS ALLL WROOONGNGNGNGNGNNG')
        row = await conn.fetchrow(
            "INSERT INTO trackresults (resultid, intendedtimespacing, intendedtimespacingunitsid, "
            "aggregationstatisticcv) VALUES ($1, $2, $3, $4) "
            "ON CONFLICT (resultid) DO UPDATE SET intendedtimespacing = EXCLUDED.intendedtimespacing returning *",
            track_result.resultid, track_result.intendedtimespacing, track_result.intendedtimespacingunitsid,
            track_result.aggregationstatisticcv)
        if track_result.track_result_locations:
            location_records = ((rec[0], shapely.wkt.loads(f"POINT({rec[1]} {rec[2]})"), rec[3],
                                 track_result.samplingfeatureid) for rec in track_result.track_result_locations)
            await conn.executemany("INSERT INTO trackresultlocations (valuedatetime, trackpoint, qualitycodecv, "
                                   "samplingfeatureid) VALUES ($1, ST_SetSRID($2::geometry, 4326), $3, $4) "
                                   "ON CONFLICT (valuedatetime, samplingfeatureid) DO UPDATE SET "
                                   "trackpoint = excluded.trackpoint, qualitycodecv = excluded.qualitycodecv",
                                   location_records)
            # result = await conn.copy_records_to_table(table_name='trackresultlocations',
            #                                           records=location_records, schema_name='odm2')
            # logging.info(result)
        if track_result.track_result_values:
            value_records = ((rec[0], rec[1], rec[2], track_result.resultid)
                             for rec in track_result.track_result_values)
            await conn.executemany("INSERT INTO trackresultvalues (valuedatetime, datavalue, qualitycodecv, resultid) "
                                   "VALUES ($1, $2, $3, $4) ON CONFLICT (valuedatetime, resultid) DO UPDATE SET "
                                   "datavalue = excluded.datavalue, qualitycodecv = excluded.qualitycodecv",
                                   value_records)
            # result = await conn.copy_records_to_table(table_name='trackresultvalues',
            #                                           records=records, schema_name='odm2')
            # logging.info(result)
    return schemas.TrackResultsReport(samplingfeatureid=track_result.samplingfeatureid,
                                      inserted_track_result_values=len(track_result.track_result_values),
                                      inserted_track_result_locations=len(track_result.track_result_locations), **row)
