import os
import logging
import asyncpg

from fastapi import FastAPI, Depends

from odm2_postgres_api.schemas import schemas
from odm2_postgres_api.queries import core_queries
from odm2_postgres_api.queries.controlled_vocabulary_queries import synchronize_cv_tables

app = FastAPI(
    docs_url="/",
    title="ODM2 API",
    version="v1"
)


class ApiPoolManager:
    def __init__(self):
        self.pool = None  # Optional[asyncpg.pool.Pool]

    async def get_conn(self) -> asyncpg.connection.Connection:
        async with self.pool.acquire() as connection:
            yield connection


api_pool_manager = ApiPoolManager()


@app.on_event("startup")
async def startup_event():
    # TODO: This can run before the database is ready, it should actually be lazily tried on the first connection
    # Get DB connection from environment
    db_host = os.environ["TIMESCALE_ODM2_SERVICE_HOST"]
    db_port = os.environ["TIMESCALE_ODM2_SERVICE_PORT"]
    db_mighty_user = os.environ["ODM2_DB_USER"]
    db_mighty_pwd = os.environ["ODM2_DB_PASSWORD"]
    db_name = os.environ["ODM2_DB"]

    logging.info("Creating connection pool")
    api_pool_manager.pool = await asyncpg.create_pool(user=db_mighty_user, password=db_mighty_pwd,
                                                      host=db_host, port=db_port, database=db_name)
    logging.info("Successfully created connection pool")


@app.on_event("shutdown")
async def shutdown_event():
    logging.info('Closing connection pool')
    await api_pool_manager.pool.close()
    logging.info('Successfully closed connection pool')


@app.patch("/controlled_vocabularies", summary="api 101 testing")
async def patch_controlled_vocabularies():
    return await synchronize_cv_tables(api_pool_manager.pool)


@app.patch("/controlled_vocabularies/{controlled_vocabulary}", summary="api 101 testing")
async def patch_single_controlled_vocabulary(controlled_vocabulary: schemas.cv_checker):  # type: ignore
    return await synchronize_cv_tables(api_pool_manager.pool, [controlled_vocabulary])


@app.post("/controlled_vocabularies", summary="api 101 testing")
async def post_controlled_vocabularies(controlled_vocabulary: schemas.ControlledVocabulary,
                                       connection=Depends(api_pool_manager.get_conn)):  # type: ignore
    return await core_queries.create_new_controlled_vocabulary_item(connection, controlled_vocabulary)


@app.post("/people", response_model=schemas.People)
async def post_people(user: schemas.PeopleCreate, connection=Depends(api_pool_manager.get_conn)):
    return await core_queries.create_person(connection, user)


@app.post("/organizations", response_model=schemas.Organizations)
async def post_organizations(user: schemas.OrganizationsCreate, connection=Depends(api_pool_manager.get_conn)):
    return await core_queries.create_organization(connection, user)


@app.post("/affiliations", response_model=schemas.Affiliations)
async def post_affiliations(affiliation: schemas.AffiliationsCreate, connection=Depends(api_pool_manager.get_conn)):
    return await core_queries.create_affiliation(connection, affiliation)


@app.post("/units", response_model=schemas.Units)
async def post_units(unit: schemas.UnitsCreate, connection=Depends(api_pool_manager.get_conn)):
    return await core_queries.create_unit(connection, unit)


@app.post("/variables", response_model=schemas.Variables)
async def post_variables(variable: schemas.VariablesCreate, connection=Depends(api_pool_manager.get_conn)):
    return await core_queries.create_variable(connection, variable)


@app.post("/equipment_model", response_model=schemas.EquipmentModelCreate)
async def post_equipment_model(equipment_model: schemas.EquipmentModelCreate,
                               connection=Depends(api_pool_manager.get_conn)):
    return await core_queries.create_equipment_model(connection, equipment_model)


@app.post("/instrument_output_variable", response_model=schemas.InstrumentOutputVariablesCreate)
async def post_instrument_output_variable(instrument_output_variable: schemas.InstrumentOutputVariablesCreate,
                                          connection=Depends(api_pool_manager.get_conn)):
    return await core_queries.create_instrument_output_variable(connection, instrument_output_variable)


@app.post("/equipment", response_model=schemas.Equipment)
async def post_equipment(equipment: schemas.EquipmentCreate, connection=Depends(api_pool_manager.get_conn)):
    return await core_queries.create_equipment(connection, equipment)


@app.post("/methods", response_model=schemas.Methods)
async def post_methods(method: schemas.MethodsCreate, connection=Depends(api_pool_manager.get_conn)):
    return await core_queries.create_method(connection, method)


@app.post("/action_by", response_model=schemas.ActionsBy)
async def post_action_by(action_by: schemas.ActionsByCreate, connection=Depends(api_pool_manager.get_conn)):
    return await core_queries.create_action_by(connection, action_by)


@app.post("/actions", response_model=schemas.Action)
async def post_actions(action_create: schemas.ActionsCreate, connection=Depends(api_pool_manager.get_conn)):
    return await core_queries.do_action(connection, action_create)


@app.post("/sampling_features", response_model=schemas.SamplingFeatures)
async def post_sampling_features(sampling_feature: schemas.SamplingFeaturesCreate,
                                 connection=Depends(api_pool_manager.get_conn)):
    return await core_queries.create_sampling_feature(connection, sampling_feature)


@app.post("/processing_levels", response_model=schemas.ProcessingLevels)
async def post_processing_levels(processing_level: schemas.ProcessingLevelsCreate,
                                 connection=Depends(api_pool_manager.get_conn)):
    return await core_queries.create_processing_level(connection, processing_level)


@app.post("/spatial_references", response_model=schemas.SpatialReferences)
async def post_spatial_references(spatial_reference: schemas.SpatialReferencesCreate,
                                  connection=Depends(api_pool_manager.get_conn)):
    return await core_queries.create_spatial_reference(connection, spatial_reference)


@app.post("/sites", response_model=schemas.Sites)
async def post_sites(site: schemas.Sites, connection=Depends(api_pool_manager.get_conn)):
    return await core_queries.create_site(connection, site)


@app.post("/data_quality", response_model=schemas.DataQuality)
async def post_data_quality(data_quality: schemas.DataQualityCreate, connection=Depends(api_pool_manager.get_conn)):
    return await core_queries.create_data_quality(connection, data_quality)


@app.post("/result_data_quality", response_model=schemas.ResultsDataQuality)
async def post_result_data_quality(data_quality_result: schemas.ResultsDataQualityCreate,
                                   connection=Depends(api_pool_manager.get_conn)):
    return await core_queries.create_result_data_quality(connection, data_quality_result)


@app.post("/feature_actions", response_model=schemas.FeatureActions)
async def post_feature_action(feature_action: schemas.FeatureActionsCreate,
                              connection=Depends(api_pool_manager.get_conn)):
    return await core_queries.create_feature_action(connection, feature_action)


@app.post("/results", response_model=schemas.Results)
async def post_results(result: schemas.ResultsCreate, connection=Depends(api_pool_manager.get_conn)):
    return await core_queries.create_result(connection, result)


@app.post("/track_results", response_model=schemas.TrackResultsReport)
async def post_track_results(track_result: schemas.TrackResultsCreate, connection=Depends(api_pool_manager.get_conn)):
    return await core_queries.upsert_track_result(connection, track_result)


@app.post("/measurement_results", response_model=schemas.MeasurementResults)
async def post_measurement_results(measurement_result: schemas.MeasurementResultsCreate,
                                   connection=Depends(api_pool_manager.get_conn)):
    return await core_queries.upsert_measurement_result(connection, measurement_result)


@app.post("/categorical_results", response_model=schemas.CategoricalResults)
async def post_categorical_results(categorical_result: schemas.CategoricalResultsCreate,
                                   connection=Depends(api_pool_manager.get_conn)):
    return await core_queries.upsert_categorical_result(connection, categorical_result)
