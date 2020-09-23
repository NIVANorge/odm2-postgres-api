import logging
from typing import Optional, List, Union
from uuid import uuid4

import asyncpg
import shapely.wkt
from fastapi import HTTPException

from odm2_postgres_api.controlled_vocabularies.download_cvs import CONTROLLED_VOCABULARY_TABLE_NAMES
from odm2_postgres_api.schemas import schemas
from odm2_postgres_api.schemas.schemas import PersonExtended, ControlledVocabulary, ControlledVocabularyCreate, \
    UnitsCreate, Units, SamplingFeatures, SamplingFeaturesCreate
from odm2_postgres_api.utils import shapely_postgres_adapter


def argument_placeholder(arguments: dict):
    return ', '.join(f'${n + 1}' for n in range(len(arguments)))  # for example: '$1, $2, $3, $4, $5'


def make_sql_query(table: str, data: dict):
    return f"INSERT INTO {table} ({','.join(data.keys())}) VALUES ({argument_placeholder(data)}) returning *"


async def insert_pydantic_object(conn: asyncpg.connection, table_name: str, pydantic_object, response_model):
    logging.debug(f"Inserting row", extra={"table": table_name})
    pydantic_dict = pydantic_object.dict()
    row = await conn.fetchrow(make_sql_query(table_name, pydantic_dict), *pydantic_dict.values())
    return response_model(**row)


async def find_unit(conn: asyncpg.connection, unit: UnitsCreate, raise_if_none=False):
    row = await conn.fetchrow(f"SELECT * FROM units WHERE unitstypecv=$1 AND unitsabbreviation=$2", unit.unitstypecv,
                              unit.unitsabbreviation)
    if row is None and raise_if_none:
        raise HTTPException(status_code=422, detail=f"Unit unitstypecv={unit.unitstypecv} and "
                                                    f"unitsabbreviation={unit.unitsabbreviation} does not exist")
    return Units(**row) if row else None


async def find_row(conn: asyncpg.connection, table: str, id_column: str, identifier, model, raise_if_none=False):
    row = await conn.fetchrow(f"SELECT * FROM {table} WHERE {id_column}=$1", identifier)
    if row is None and raise_if_none:
        raise HTTPException(status_code=422, detail=f"Item in table {table} with '{id_column}={identifier}' not found")
    return model(**row) if row else None


# TODO: we may want to extend this further by including organization ++
async def find_person_by_external_id(conn: asyncpg.connection, external_system, ext_id: str) -> PersonExtended:
    row = await conn.fetchrow("SELECT p.*, a.affiliationid, a.primaryemail, "
                              "pe.externalidentifiersystemid, eis.externalidentifiersystemname FROM people p "
                              "inner join affiliations a on p.personid=a.personid "
                              "inner join personexternalidentifiers pe on p.personid = pe.personid "
                              "inner join externalidentifiersystems eis on "
                              "eis.externalidentifiersystemid=pe.externalidentifiersystemid "
                              "where eis.externalidentifiersystemname = $1 "
                              "AND LOWER(pe.personexternalidentifier)=LOWER($2)",
                              external_system, ext_id)

    if row:
        return PersonExtended(**row)
    raise HTTPException(status_code=404, detail=f"Person with active directory username "
                                                f"'{ext_id}' not found.")


async def create_new_controlled_vocabulary_item(conn: asyncpg.connection,
                                                controlled_vocabulary: ControlledVocabularyCreate) \
        -> ControlledVocabulary:
    table_name = controlled_vocabulary.controlled_vocabulary_table_name
    # Check against hardcoded table names otherwise this could be an SQL injection
    if table_name not in CONTROLLED_VOCABULARY_TABLE_NAMES:
        raise RuntimeError(f"table_name: '{table_name}' is invalid")
    value_keys = ['term', 'name', 'definition', 'category']
    controlled_vocabulary_data = {k: v for k, v in controlled_vocabulary if k in value_keys}
    existing = await find_row(conn, table_name, "term", controlled_vocabulary.term, ControlledVocabulary)
    if existing:
        return existing

    row = await conn.fetchrow(make_sql_query(table_name, controlled_vocabulary_data),
                              *controlled_vocabulary_data.values())
    return schemas.ControlledVocabulary(**row)


async def create_or_parse_annotations(conn: asyncpg.connection,
                                      annotations: List[Union[schemas.AnnotationsCreate, int]]):
    annotation_ids = []
    for annotation in annotations:
        if type(annotation) == schemas.AnnotationsCreate:
            annotation_row = await insert_pydantic_object(conn, 'annotations', annotation, schemas.Annotations)
            annotation_ids.append(annotation_row.annotationid)
        elif type(annotation) == int:  # No need for 'else' clause with TypeError since fastapi already checked
            annotation_ids.append(annotation)
    return annotation_ids


async def insert_taxonomic_classifier(conn: asyncpg.connection, taxon: schemas.TaxonomicClassifierCreate):
    taxon_data = {k: v for k, v in taxon if k != "annotations"}
    async with conn.transaction():
        taxon_row = await conn.fetchrow(make_sql_query('taxonomicclassifiers', taxon_data), *taxon_data.values())
        logging.info(taxon_row)
        for annotation_id in await create_or_parse_annotations(conn, taxon.annotations):
            await conn.fetchrow('INSERT INTO TaxonomicClassifiersAnnotations (taxonomicclassifierid, annotationid) '
                                'Values ($1, $2) returning *',
                                taxon_row['taxonomicclassifierid'], annotation_id)
    return schemas.TaxonomicClassifier(annotations=taxon.annotations, **taxon_row)


async def insert_method(conn: asyncpg.connection, method: schemas.MethodsCreate):
    method_data = {k: v for k, v in method if k != "annotations"}
    async with conn.transaction():
        method_row = await conn.fetchrow(make_sql_query('methods', method_data), *method_data.values())
        for annotation_id in await create_or_parse_annotations(conn, method.annotations):
            await conn.fetchrow('INSERT INTO methodannotations (methodid, annotationid) Values ($1, $2) returning *',
                                method_row['methodid'], annotation_id)
    return schemas.Methods(annotations=method.annotations, **method_row)


async def do_action(conn: asyncpg.connection, action: schemas.ActionsCreate) -> schemas.Action:
    async with conn.transaction():
        method_row = await conn.fetchrow("SELECT methodid FROM methods WHERE methodcode = $1", action.methodcode)
        if method_row is None:
            raise HTTPException(status_code=422, detail="Please specify valid methodcode.")
        action_row = await conn.fetchrow(
            "INSERT INTO actions (actiontypecv, methodid, begindatetime, begindatetimeutcoffset,  enddatetime, "
            "enddatetimeutcoffset, actiondescription, actionfilelink) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7, $8) returning *",
            action.actiontypecv, method_row['methodid'], action.begindatetime, action.begindatetimeutcoffset,
            action.enddatetime, action.enddatetimeutcoffset, action.actiondescription, action.actionfilelink)

        action_by = schemas.ActionsByCreate(actionid=action_row['actionid'], affiliationid=action.affiliationid,
                                            isactionlead=action.isactionlead, roledescription=action.roledescription)
        action_by_row = await insert_pydantic_object(conn, 'actionby', action_by, schemas.ActionsBy)
        for equipmentid in action.equipmentids:
            equipment_used_create = schemas.EquipmentUsedCreate(actionid=action_row['actionid'],
                                                                equipmentid=equipmentid)
            await insert_pydantic_object(conn, 'equipmentused', equipment_used_create, schemas.EquipmentUsed)

        for directiveid in action.directiveids:
            action_directive_create = schemas.ActionDirectivesCreate(actionid=action_row['actionid'],
                                                                     directiveid=directiveid)
            await insert_pydantic_object(conn, 'actiondirectives', action_directive_create, schemas.ActionDirective)

        for action_id, relation_ship_type in action.relatedactions:
            related_action_create = schemas.RelatedActionCreate(
                actionid=action_row['actionid'], relationshiptypecv=relation_ship_type, relatedactionid=action_id)
            await insert_pydantic_object(conn, 'relatedactions', related_action_create, schemas.RelatedAction)

        new_sampling_features = []
        for sampling_feature in action.sampling_features:
            new_sampling_features.append(await create_sampling_feature(conn, sampling_feature))

            feature_action = schemas.FeatureActionsCreate(
                samplingfeatureuuid=new_sampling_features[-1].samplingfeatureuuid, actionid=action_row['actionid'])
            await create_feature_action(conn, feature_action)
    # Dict allows overwriting of key while pydantic schema does not, identical action_id exists in both return rows
    return schemas.Action(equipmentids=action.equipmentids, methodcode=action.methodcode,
                          sampling_features=new_sampling_features, **{**action_row, **dict(action_by_row)})


async def create_sampling_feature(conn: asyncpg.connection, sampling_feature: schemas.SamplingFeaturesCreate):
    # Todo: Find a better way to deal with 'Null' geometry
    if sampling_feature.featuregeometrywkt:
        await shapely_postgres_adapter.set_shapely_adapter(conn)
        featuregeometry = shapely.wkt.loads(sampling_feature.featuregeometrywkt)
    else:
        featuregeometry = None

    async with conn.transaction():
        sampling_row = await conn.fetchrow(
            "INSERT INTO samplingfeatures (samplingfeatureuuid, samplingfeaturetypecv, samplingfeaturecode, "
            "samplingfeaturename, samplingfeaturedescription, samplingfeaturegeotypecv, featuregeometry, "
            "featuregeometrywkt, elevation_m, elevationdatumcv) "
            "VALUES ($1, $2, $3, $4, $5, $6, "
            "ST_SetSRID($7::geometry, 4326), $8, $9, $10) returning *",
            sampling_feature.samplingfeatureuuid, sampling_feature.samplingfeaturetypecv,
            sampling_feature.samplingfeaturecode, sampling_feature.samplingfeaturename,
            sampling_feature.samplingfeaturedescription, sampling_feature.samplingfeaturegeotypecv,
            featuregeometry, sampling_feature.featuregeometrywkt,
            sampling_feature.elevation_m, sampling_feature.elevationdatumcv
        )
        for sampling_feature_id, relation_ship_type in sampling_feature.relatedsamplingfeatures:
            related_sampling_feature_create = schemas.RelatedSamplingFeatureCreate(
                samplingfeatureid=sampling_row['samplingfeatureid'], relationshiptypecv=relation_ship_type,
                relatedfeatureid=sampling_feature_id,
            )
            await insert_pydantic_object(conn, 'relatedfeatures', related_sampling_feature_create,
                                         schemas.RelatedSamplingFeature)
        for annotation_id in await create_or_parse_annotations(conn, sampling_feature.annotations):
            await conn.fetchrow('INSERT INTO samplingfeatureannotations (samplingfeatureid, annotationid) '
                                'Values ($1, $2) returning *', sampling_row['samplingfeatureid'], annotation_id)
    return schemas.SamplingFeatures(**sampling_row)


async def create_sampling_feature_annotation(conn: asyncpg.connection,
                                             sampling_feature_annotation: schemas.SamplingFeatureAnnotationCreate):
    if sampling_feature_annotation.annotationid is None:
        annotation_id = await create_or_parse_annotations(conn, [
            schemas.AnnotationsCreate(**sampling_feature_annotation.dict())])
        sampling_feature_annotation.annotationid = annotation_id[0]

    await conn.fetchrow(
        "INSERT INTO samplingfeatureannotations (samplingfeatureid, annotationid) "
        "VALUES ($1, $2) returning * ", sampling_feature_annotation.samplingfeatureid,
        sampling_feature_annotation.annotationid)

    return sampling_feature_annotation


async def create_result_data_quality(conn: asyncpg.connection, result_data_quality: schemas.ResultsDataQualityCreate):
    row = await conn.fetchrow(
        "INSERT INTO resultsdataquality (resultid, dataqualityid) "
        "VALUES ($1, (SELECT dataqualityid FROM dataquality where dataqualitycode = $2)) "
        "ON CONFLICT (resultid, dataqualityid) DO UPDATE SET resultid = EXCLUDED.resultid returning *",
        result_data_quality.resultid, result_data_quality.dataqualitycode)
    return schemas.ResultsDataQuality(dataqualitycode=result_data_quality.dataqualitycode, **row)


async def create_feature_action(conn: asyncpg.connection, feature_action: schemas.FeatureActionsCreate):
    if feature_action.samplingfeatureuuid and feature_action.samplingfeaturecode:
        row = await conn.fetchrow(
            "INSERT INTO featureactions (samplingfeatureid, actionid) "
            "VALUES ((SELECT samplingfeatureid FROM samplingfeatures "
            "where samplingfeatureuuid = $1 AND samplingfeaturecode = $2), $3) "
            "ON CONFLICT (samplingfeatureid, actionid) DO UPDATE SET actionid = EXCLUDED.actionid returning *",
            feature_action.samplingfeatureuuid, feature_action.samplingfeaturecode, feature_action.actionid)
        return schemas.FeatureActions(samplingfeatureuuid=feature_action.samplingfeatureuuid, **row)
    else:
        row = await conn.fetchrow(
            "INSERT INTO featureactions (samplingfeatureid, actionid) "
            "VALUES ((SELECT samplingfeatureid FROM samplingfeatures "
            "where samplingfeatureuuid = $1 OR samplingfeaturecode = $2), $3) "
            "ON CONFLICT (samplingfeatureid, actionid) DO UPDATE SET actionid = EXCLUDED.actionid returning *",
            feature_action.samplingfeatureuuid, feature_action.samplingfeaturecode, feature_action.actionid)
        return schemas.FeatureActions(samplingfeatureuuid=feature_action.samplingfeatureuuid,
                                      samplingfeaturecode=feature_action.samplingfeaturecode, **row)


async def create_result(conn: asyncpg.connection, result: schemas.ResultsCreate):
    async with conn.transaction():
        feature_action_create = schemas.FeatureActionsCreate(samplingfeatureuuid=result.samplingfeatureuuid,
                                                             samplingfeaturecode=result.samplingfeaturecode,
                                                             actionid=result.actionid)
        feature_action_row = await create_feature_action(conn, feature_action_create)
        result_row = await conn.fetchrow(
            "INSERT INTO results (resultuuid, featureactionid, resulttypecv, variableid, unitsid,"
            "taxonomicclassifierid, processinglevelid, resultdatetime, resultdatetimeutcoffset, validdatetime,"
            "validdatetimeutcoffset, statuscv, sampledmediumcv, valuecount) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14) returning *",
            result.resultuuid, feature_action_row.featureactionid, result.resulttypecv, result.variableid,
            result.unitsid, result.taxonomicclassifierid, result.processinglevelid, result.resultdatetime,
            result.resultdatetimeutcoffset, result.validdatetime, result.validdatetimeutcoffset, result.statuscv,
            result.sampledmediumcv, result.valuecount)
        for data_quality_code in result.dataqualitycodes:
            await create_result_data_quality(conn, schemas.ResultsDataQualityCreate(
                resultid=result_row['resultid'], dataqualitycode=data_quality_code))
        for annotation_id in await create_or_parse_annotations(conn, result.annotations):
            await conn.fetchrow('INSERT INTO resultannotations (resultid, annotationid, begindatetime, enddatetime) '
                                'Values ($1, $2, $3, $4) returning *',
                                result_row["resultid"], annotation_id, result.resultdatetime, result.validdatetime)
    # Dict allows overwriting of key while pydantic schema does not, featureactionid exists in both return rows
    return schemas.Results(dataqualitycodes=result.dataqualitycodes, **{**result_row, **dict(feature_action_row)})


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


async def upsert_measurement_result(conn: asyncpg.connection, measurement_result: schemas.MeasurementResultsCreate):
    async with conn.transaction():
        value_keys = ['datavalue', 'valuedatetime', 'valuedatetimeutcoffset']
        measurementresults_data = {k: v for k, v in measurement_result if k not in value_keys}
        measurementresultvalues_data = {k: v for k, v in measurement_result if k in value_keys or k == 'resultid'}

        await conn.fetchrow(make_sql_query('measurementresults', measurementresults_data),
                            *measurementresults_data.values())
        await conn.fetchrow(make_sql_query('measurementresultvalues', measurementresultvalues_data),
                            *measurementresultvalues_data.values())


async def upsert_categorical_result(conn: asyncpg.connection, categorical_result: schemas.CategoricalResultsCreate):
    async with conn.transaction():
        value_keys = ['datavalue', 'valuedatetime', 'valuedatetimeutcoffset']
        categoricalresults_data = {k: v for k, v in categorical_result if k not in value_keys}
        categoricalresultvalues_data = {k: v for k, v in categorical_result if k in value_keys or k == 'resultid'}

        await conn.fetchrow(make_sql_query('categoricalresults', categoricalresults_data),
                            *categoricalresults_data.values())
        await conn.fetchrow(make_sql_query('categoricalresultvalues', categoricalresultvalues_data),
                            *categoricalresultvalues_data.values())


async def find_or_create_sampling_feature(conn, code: str, sf_type: str, wkt: str = None) -> SamplingFeatures:
    existing = await find_row(
        conn,
        "samplingfeatures",
        "samplingfeaturecode",
        code,
        SamplingFeatures,
    )

    if existing:
        return existing

    new_sf = SamplingFeaturesCreate(
        samplingfeatureuuid=uuid4(),
        samplingfeaturetypecv=sf_type,
        samplingfeaturecode=code,
        featuregeometrywkt=wkt
    )

    return await create_sampling_feature(conn, sampling_feature=new_sf)
