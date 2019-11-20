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


@app.post("/controlled_vocabularies/{controlled_vocabulary}", summary="api 101 testing")
async def post_controlled_vocabularies(controlled_vocabulary: schemas.cv_checker):  # type: ignore
    return await synchronize_cv_tables(api_pool_manager.pool, [controlled_vocabulary])


@app.post("/people", response_model=schemas.People)
async def post_people(user: schemas.PeopleCreate, connection=Depends(api_pool_manager.get_conn)):
    return await core_queries.create_person(connection, user)


@app.post("/organizations", response_model=schemas.Organizations)
async def post_organizations(user: schemas.OrganizationsCreate, connection=Depends(api_pool_manager.get_conn)):
    return await core_queries.create_organization(connection, user)


@app.post("/affiliations", response_model=schemas.Affiliations)
async def post_affiliations(affiliation: schemas.AffiliationsCreate, connection=Depends(api_pool_manager.get_conn)):
    return await core_queries.create_affiliation(connection, affiliation)


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


@app.post("/spatial_references", response_model=schemas.SpatialReferences)
async def post_spatial_references(spatial_reference: schemas.SpatialReferencesCreate,
                                  connection=Depends(api_pool_manager.get_conn)):
    return await core_queries.create_spatial_reference(connection, spatial_reference)


@app.post("/sites", response_model=schemas.Sites)
async def post_sites(site: schemas.Sites, connection=Depends(api_pool_manager.get_conn)):
    return await core_queries.create_site(connection, site)


@app.post("/processing_levels", response_model=schemas.ProcessingLevels)
async def post_processing_levels(processing_level: schemas.ProcessingLevelsCreate,
                                 connection=Depends(api_pool_manager.get_conn)):
    return await core_queries.create_processing_level(connection, processing_level)


@app.post("/units", response_model=schemas.Units)
async def post_units(unit: schemas.UnitsCreate, connection=Depends(api_pool_manager.get_conn)):
    return await core_queries.create_unit(connection, unit)


@app.post("/variables", response_model=schemas.Variables)
async def post_variables(variable: schemas.VariablesCreate, connection=Depends(api_pool_manager.get_conn)):
    return await core_queries.create_variable(connection, variable)


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
