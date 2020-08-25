from odm2_postgres_api.queries.core_queries import insert_pydantic_object, find_person_by_external_id
from odm2_postgres_api.schemas import schemas
from odm2_postgres_api.queries import core_queries
from odm2_postgres_api.queries.controlled_vocabulary_queries import synchronize_cv_tables
from odm2_postgres_api.schemas.schemas import PersonExtended
from fastapi import Depends, APIRouter
from odm2_postgres_api.utils.api_pool_manager import api_pool_manager


router = APIRouter()


@router.patch("/controlled_vocabularies", summary="api 101 testing")
async def patch_controlled_vocabularies():
    return await synchronize_cv_tables(api_pool_manager.pool)


@router.patch("/controlled_vocabularies/{controlled_vocabulary}", summary="api 101 testing")
async def patch_single_controlled_vocabulary(controlled_vocabulary: schemas.cv_checker):  # type: ignore
    return await synchronize_cv_tables(api_pool_manager.pool, [controlled_vocabulary])


@router.post("/controlled_vocabularies", summary="api 101 testing")
async def post_controlled_vocabularies(controlled_vocabulary: schemas.ControlledVocabulary,
                                       connection=Depends(api_pool_manager.get_conn)):  # type: ignore
    return await core_queries.create_new_controlled_vocabulary_item(connection, controlled_vocabulary)


@router.post("/annotations", response_model=schemas.Annotations)
async def post_annotations(annotation: schemas.AnnotationsCreate, connection=Depends(api_pool_manager.get_conn)):
    return await insert_pydantic_object(connection, 'annotations', annotation, schemas.Annotations)


@router.post("/people", response_model=schemas.People)
async def post_people(user: schemas.PeopleCreate, connection=Depends(api_pool_manager.get_conn)):
    return await insert_pydantic_object(connection, 'people', user, schemas.People)


@router.get("/people/active-directory/{sam_account_name}", response_model=PersonExtended)
async def get_person_by_ad_sam_acc_name(sam_account_name: str, connection=Depends(api_pool_manager.get_conn)):
    """Retrieves users based on their Active Directory 3-letter username (SamAccountName)"""
    return await find_person_by_external_id(connection, "onprem-active-directory", sam_account_name)


@router.post("/organizations", response_model=schemas.Organizations)
async def post_organizations(organization: schemas.OrganizationsCreate, connection=Depends(api_pool_manager.get_conn)):
    return await insert_pydantic_object(connection, 'organizations', organization, schemas.Organizations)


@router.post("/external_identifier_system", response_model=schemas.ExternalIdentifierSystems)
async def post_external_identifier_system(external_identifier_system: schemas.ExternalIdentifierSystemsCreate,
                                          connection=Depends(api_pool_manager.get_conn)):
    return await insert_pydantic_object(connection, 'externalidentifiersystems',
                                        external_identifier_system, schemas.ExternalIdentifierSystems)


@router.post("/affiliations", response_model=schemas.Affiliations)
async def post_affiliations(affiliation: schemas.AffiliationsCreate, connection=Depends(api_pool_manager.get_conn)):
    return await insert_pydantic_object(connection, 'affiliations', affiliation, schemas.Affiliations)


@router.post("/units", response_model=schemas.Units)
async def post_units(unit: schemas.UnitsCreate, connection=Depends(api_pool_manager.get_conn)):
    return await insert_pydantic_object(connection, 'units', unit, schemas.Units)


@router.post("/variables", response_model=schemas.Variables)
async def post_variables(variable: schemas.VariablesCreate, connection=Depends(api_pool_manager.get_conn)):
    return await insert_pydantic_object(connection, 'variables', variable, schemas.Variables)


@router.post("/equipment_model", response_model=schemas.EquipmentModelCreate)
async def post_equipment_model(equipment_model: schemas.EquipmentModelCreate,
                               connection=Depends(api_pool_manager.get_conn)):
    return await insert_pydantic_object(connection, 'equipmentmodels', equipment_model, schemas.EquipmentModelCreate)


@router.post("/instrument_output_variable", response_model=schemas.InstrumentOutputVariablesCreate)
async def post_instrument_output_variable(instrument_output_variable: schemas.InstrumentOutputVariablesCreate,
                                          connection=Depends(api_pool_manager.get_conn)):
    return await insert_pydantic_object(connection, 'instrumentoutputvariables',
                                        instrument_output_variable, schemas.InstrumentOutputVariablesCreate)


@router.post("/equipment", response_model=schemas.Equipment)
async def post_equipment(equipment: schemas.EquipmentCreate, connection=Depends(api_pool_manager.get_conn)):
    return await insert_pydantic_object(connection, 'equipment', equipment, schemas.Equipment)


@router.post("/directives", response_model=schemas.Directive)
async def post_directive(directive: schemas.DirectivesCreate, connection=Depends(api_pool_manager.get_conn)):
    return await insert_pydantic_object(connection, 'directives', directive, schemas.Directive)


@router.post("/methods", response_model=schemas.Methods)
async def post_methods(method: schemas.MethodsCreate, connection=Depends(api_pool_manager.get_conn)):
    return await core_queries.insert_method(connection, method)


@router.post("/action_by", response_model=schemas.ActionsBy)
async def post_action_by(action_by: schemas.ActionsByCreate, connection=Depends(api_pool_manager.get_conn)):
    return await insert_pydantic_object(connection, 'actionby', action_by, schemas.ActionsBy)


@router.post("/actions", response_model=schemas.Action)
async def post_actions(action_create: schemas.ActionsCreate, connection=Depends(api_pool_manager.get_conn)):
    return await core_queries.do_action(connection, action_create)


@router.post("/sampling_features", response_model=schemas.SamplingFeatures)
async def post_sampling_features(sampling_feature: schemas.SamplingFeaturesCreate,
                                 connection=Depends(api_pool_manager.get_conn)):
    return await core_queries.create_sampling_feature(connection, sampling_feature)


@router.post("/processing_levels", response_model=schemas.ProcessingLevels)
async def post_processing_levels(processing_level: schemas.ProcessingLevelsCreate,
                                 connection=Depends(api_pool_manager.get_conn)):
    return await insert_pydantic_object(connection, 'processinglevels', processing_level, schemas.ProcessingLevels)


@router.post("/spatial_references", response_model=schemas.SpatialReferences)
async def post_spatial_references(spatial_reference: schemas.SpatialReferencesCreate,
                                  connection=Depends(api_pool_manager.get_conn)):
    return await insert_pydantic_object(connection, 'spatialreferences', spatial_reference, schemas.SpatialReferences)


@router.post("/sites", response_model=schemas.Sites)
async def post_sites(site: schemas.Sites, connection=Depends(api_pool_manager.get_conn)):
    return await insert_pydantic_object(connection, 'sites', site, schemas.Sites)


@router.post("/data_quality", response_model=schemas.DataQuality)
async def post_data_quality(data_quality: schemas.DataQualityCreate, connection=Depends(api_pool_manager.get_conn)):
    return await insert_pydantic_object(connection, 'dataquality', data_quality, schemas.DataQuality)


@router.post("/taxonomic_classifiers", response_model=schemas.TaxonomicClassifier)
async def post_taxonomic_classifiers(taxonomic_classifier_create: schemas.TaxonomicClassifierCreate,
                                     connection=Depends(api_pool_manager.get_conn)):
    return await insert_pydantic_object(connection, 'taxonomicclassifiers',
                                        taxonomic_classifier_create, schemas.TaxonomicClassifier)


@router.post("/result_data_quality", response_model=schemas.ResultsDataQuality)
async def post_result_data_quality(data_quality_result: schemas.ResultsDataQualityCreate,
                                   connection=Depends(api_pool_manager.get_conn)):
    return await core_queries.create_result_data_quality(connection, data_quality_result)


@router.post("/feature_actions", response_model=schemas.FeatureActions)
async def post_feature_action(feature_action: schemas.FeatureActionsCreate,
                              connection=Depends(api_pool_manager.get_conn)):
    return await core_queries.create_feature_action(connection, feature_action)


@router.post("/results", response_model=schemas.Results)
async def post_results(result: schemas.ResultsCreate, connection=Depends(api_pool_manager.get_conn)):
    return await core_queries.create_result(connection, result)


@router.post("/track_results", response_model=schemas.TrackResultsReport)
async def post_track_results(track_result: schemas.TrackResultsCreate, connection=Depends(api_pool_manager.get_conn)):
    return await core_queries.upsert_track_result(connection, track_result)


@router.post("/measurement_results", response_model=schemas.MeasurementResults)
async def post_measurement_results(measurement_result: schemas.MeasurementResultsCreate,
                                   connection=Depends(api_pool_manager.get_conn)):
    return await core_queries.upsert_measurement_result(connection, measurement_result)


@router.post("/categorical_results", response_model=schemas.CategoricalResults)
async def post_categorical_results(categorical_result: schemas.CategoricalResultsCreate,
                                   connection=Depends(api_pool_manager.get_conn)):
    return await core_queries.upsert_categorical_result(connection, categorical_result)
