import os
import logging
import uuid
from collections import defaultdict

import asyncpg

from fastapi import FastAPI, Depends, Header
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.status import HTTP_422_UNPROCESSABLE_ENTITY

from odm2_postgres_api.queries.core_queries import insert_pydantic_object
from odm2_postgres_api.queries.user import create_or_get_user
from odm2_postgres_api.schemas import schemas
from odm2_postgres_api.queries import core_queries
from odm2_postgres_api.queries.controlled_vocabulary_queries import synchronize_cv_tables
from odm2_postgres_api.utils import google_cloud_utils

app = FastAPI(
    docs_url="/",
    title="ODM2 API",
    version="v1"
)


@app.exception_handler(RequestValidationError)
async def request_validation_exception_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    # Log the error that is send to the client
    logging.info('Error thrown on request', extra={'url': request.url, 'error_detail': jsonable_encoder(exc.errors())})
    return JSONResponse(
        status_code=HTTP_422_UNPROCESSABLE_ENTITY,
        content={"detail": jsonable_encoder(exc.errors()), "body": exc.body},
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


@app.post("/annotations", response_model=schemas.Annotations)
async def post_annotations(annotation: schemas.AnnotationsCreate, connection=Depends(api_pool_manager.get_conn)):
    return await insert_pydantic_object(connection, 'annotations', annotation, schemas.Annotations)


@app.post("/people", response_model=schemas.People)
async def post_people(user: schemas.PeopleCreate, connection=Depends(api_pool_manager.get_conn)):
    return await insert_pydantic_object(connection, 'people', user, schemas.People)


@app.post("/organizations", response_model=schemas.Organizations)
async def post_organizations(organization: schemas.OrganizationsCreate, connection=Depends(api_pool_manager.get_conn)):
    return await insert_pydantic_object(connection, 'organizations', organization, schemas.Organizations)


@app.post("/external_identifier_system", response_model=schemas.ExternalIdentifierSystems)
async def post_external_identifier_system(external_identifier_system: schemas.ExternalIdentifierSystemsCreate,
                                          connection=Depends(api_pool_manager.get_conn)):
    return await insert_pydantic_object(connection, 'externalidentifiersystems',
                                        external_identifier_system, schemas.ExternalIdentifierSystems)


@app.post("/affiliations", response_model=schemas.Affiliations)
async def post_affiliations(affiliation: schemas.AffiliationsCreate, connection=Depends(api_pool_manager.get_conn)):
    return await insert_pydantic_object(connection, 'affiliations', affiliation, schemas.Affiliations)


@app.post("/units", response_model=schemas.Units)
async def post_units(unit: schemas.UnitsCreate, connection=Depends(api_pool_manager.get_conn)):
    return await insert_pydantic_object(connection, 'units', unit, schemas.Units)


@app.post("/variables", response_model=schemas.Variables)
async def post_variables(variable: schemas.VariablesCreate, connection=Depends(api_pool_manager.get_conn)):
    return await insert_pydantic_object(connection, 'variables', variable, schemas.Variables)


@app.post("/equipment_model", response_model=schemas.EquipmentModelCreate)
async def post_equipment_model(equipment_model: schemas.EquipmentModelCreate,
                               connection=Depends(api_pool_manager.get_conn)):
    return await insert_pydantic_object(connection, 'equipmentmodels', equipment_model, schemas.EquipmentModelCreate)


@app.post("/instrument_output_variable", response_model=schemas.InstrumentOutputVariablesCreate)
async def post_instrument_output_variable(instrument_output_variable: schemas.InstrumentOutputVariablesCreate,
                                          connection=Depends(api_pool_manager.get_conn)):
    return await insert_pydantic_object(connection, 'instrumentoutputvariables',
                                        instrument_output_variable, schemas.InstrumentOutputVariablesCreate)


@app.post("/equipment", response_model=schemas.Equipment)
async def post_equipment(equipment: schemas.EquipmentCreate, connection=Depends(api_pool_manager.get_conn)):
    return await insert_pydantic_object(connection, 'equipment', equipment, schemas.Equipment)


@app.post("/directives", response_model=schemas.Directive)
async def post_directive(directive: schemas.DirectivesCreate, connection=Depends(api_pool_manager.get_conn)):
    return await insert_pydantic_object(connection, 'directives', directive, schemas.Directive)


@app.post("/methods", response_model=schemas.Methods)
async def post_methods(method: schemas.MethodsCreate, connection=Depends(api_pool_manager.get_conn)):
    return await core_queries.insert_method(connection, method)


@app.post("/action_by", response_model=schemas.ActionsBy)
async def post_action_by(action_by: schemas.ActionsByCreate, connection=Depends(api_pool_manager.get_conn)):
    return await insert_pydantic_object(connection, 'actionby', action_by, schemas.ActionsBy)


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
    return await insert_pydantic_object(connection, 'processinglevels', processing_level, schemas.ProcessingLevels)


@app.post("/spatial_references", response_model=schemas.SpatialReferences)
async def post_spatial_references(spatial_reference: schemas.SpatialReferencesCreate,
                                  connection=Depends(api_pool_manager.get_conn)):
    return await insert_pydantic_object(connection, 'spatialreferences', spatial_reference, schemas.SpatialReferences)


@app.post("/sites", response_model=schemas.Sites)
async def post_sites(site: schemas.Sites, connection=Depends(api_pool_manager.get_conn)):
    return await insert_pydantic_object(connection, 'sites', site, schemas.Sites)


@app.post("/data_quality", response_model=schemas.DataQuality)
async def post_data_quality(data_quality: schemas.DataQualityCreate, connection=Depends(api_pool_manager.get_conn)):
    return await insert_pydantic_object(connection, 'dataquality', data_quality, schemas.DataQuality)


@app.post("/taxonomic_classifiers", response_model=schemas.TaxonomicClassifier)
async def post_taxonomic_classifiers(taxonomic_classifier_create: schemas.TaxonomicClassifierCreate,
                                     connection=Depends(api_pool_manager.get_conn)):
    return await insert_pydantic_object(connection, 'taxonomicclassifiers',
                                        taxonomic_classifier_create, schemas.TaxonomicClassifier)


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


@app.post("/begroing_result", response_model=schemas.BegroingResult)
async def post_begroing_result(begroing_result: schemas.BegroingResultCreate,
                               connection=Depends(api_pool_manager.get_conn),
                               niva_user: str = Header(None)):
    user = await create_or_get_user(connection, niva_user)

    csv_data = google_cloud_utils.generate_csv_from_form(begroing_result)
    if not csv_data:
        return schemas.BegroingResult(personid=user.person.personid, **begroing_result.dict())
    observations_per_method = defaultdict(list)
    for index, species in enumerate(begroing_result.taxons):
        used_method_indices = [i for i, e in enumerate(begroing_result.observations[index]) if e]
        if len(used_method_indices) != 1:
            raise ValueError('Must have one and only one method per species')
        observations_per_method[used_method_indices[0]].append(index)

    # 13 is 'Microscopic semi quantitative abundance classifier', 17 is a percentage
    result_type_and_unit_dict = {
        'Microscopic abundance': ("Category observation", 13, "Liquid aqueous"),
        'Macroscopic coverage': ("Measurement", 17, "Vegetation")
    }
    async with connection.transaction():
        for method_index, method_observations in observations_per_method.items():
            method = begroing_result.methods[method_index]

            data_action = schemas.ActionsCreate(
                affiliationid=user.affiliation.affiliationid,
                isactionlead=True,
                methodcode=method.methodcode,
                actiontypecv=method.methodtypecv,  # This only works when the type is both an action and a method
                begindatetime=begroing_result.date,
                begindatetimeutcoffset=0,
                equipmentids=[],
                directiveids=[e.directiveid for e in begroing_result.projects]
            )
            completed_action = await post_actions(data_action, connection)
            for result_index in method_observations:
                data_result = schemas.ResultsCreate(
                    samplingfeatureuuid=begroing_result.station['samplingfeatureuuid'],
                    actionid=completed_action.actionid,
                    resultuuid=str(uuid.uuid4()),
                    resulttypecv=result_type_and_unit_dict[method.methodname][0],
                    variableid=10,  # This variable indicates the abundance of the taxon of the result
                    unitsid=result_type_and_unit_dict[method.methodname][1],
                    taxonomicclassifierid=begroing_result.taxons[result_index]['taxonomicclassifierid'],
                    processinglevelid=1,  # id:1, "processinglevelcode": "0", "definition": "Raw Data"
                    valuecount=0,
                    statuscv="Complete",
                    sampledmediumcv=result_type_and_unit_dict[method.methodname][2],
                    dataqualitycodes=[]
                )
                completed_result = await post_results(data_result, connection)
                if method.methodname == 'Microscopic abundance':
                    data_categorical_result = schemas.CategoricalResultsCreate(
                        resultid=completed_result.resultid,
                        qualitycodecv="None",
                        datavalue=begroing_result.observations[result_index][method_index],
                        valuedatetime=begroing_result.date,
                        valuedatetimeutcoffset=0,
                    )
                    await post_categorical_results(data_categorical_result, connection)
                else:
                    if begroing_result.observations[result_index][method_index][0] == '<':
                        data_value = begroing_result.observations[result_index][method_index][1:]
                        censor_code = "Less than"
                    else:
                        data_value = begroing_result.observations[result_index][method_index]
                        censor_code = "Not censored"
                    data_measurement_result = schemas.MeasurementResultsCreate(
                        resultid=completed_result.resultid,
                        censorcodecv=censor_code,
                        qualitycodecv="None",
                        aggregationstatisticcv="Sporadic",
                        timeaggregationinterval=0,
                        timeaggregationintervalunitsid=18,  # time in seconds
                        datavalue=data_value,
                        valuedatetime=begroing_result.date,
                        valuedatetimeutcoffset=0
                    )
                    await post_measurement_results(data_measurement_result, connection)
        google_cloud_utils.put_csv_to_bucket(csv_data)
    # TODO: Send email about new bucket_files

    return schemas.BegroingResult(personid=user.person.personid, **begroing_result.dict())
