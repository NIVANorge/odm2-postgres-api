from fastapi import Depends, APIRouter
from odm2_postgres_api.utils.api_pool_manager import api_pool_manager

from odm2_postgres_api.queries.mass_spec_select_queries import get_sample_uuid_through_result_annotation_link, \
    get_sample_annotations_through_sample_uuid, get_parent_feature_uuid, \
    get_result_annotationlink_through_sample_uuid, get_method_annotation_through_method_code, \
    get_annotationlink_through_sample_and_method, get_replicas_uuid_from_samplingfeaturecode, \
    get_annotationlink_through_sample_and_methods, get_samplingfeatureuuid_from_samplingfeaturecode


router = APIRouter()


@router.get("/samplingfeature_uuid_through_result_annotation_link{annotationlink}")
async def get_uuid(annotationlink: str, connection=Depends(api_pool_manager.get_conn)):
    return await get_sample_uuid_through_result_annotation_link(connection, annotationlink)


@router.get("/sample_annotations_through_sample_uuid{uuid}")
async def get_processing_parameters(uuid: str, connection=Depends(api_pool_manager.get_conn)):
    return await get_sample_annotations_through_sample_uuid(connection, uuid)


@router.get("/parent_feature_uuid{uuid}")
async def get_parent_uuid(uuid: str, connection=Depends(api_pool_manager.get_conn)):
    return await get_parent_feature_uuid(connection, uuid)


@router.get("/result_annotationlink_through_sample_uuid_and_code{uuid}/{code}")
async def get_file_location(uuid: str, code: str, connection=Depends(api_pool_manager.get_conn)):
    return await get_result_annotationlink_through_sample_uuid(connection, uuid, code)


@router.get("/method_annotation_through_method_code{code}")
async def get_method_parameters(code: str, connection=Depends(api_pool_manager.get_conn)):
    return await get_method_annotation_through_method_code(connection, code)


@router.get("/get_location_via_method{uuid}/{methodcode}/{annotationcode}")
async def get_file_location_via_method(uuid: str, methodcode: str, annotationcode: str,
                                       connection=Depends(api_pool_manager.get_conn)):
    return await get_annotationlink_through_sample_and_method(connection, uuid, methodcode, annotationcode)


@router.get("/get_location_via_methods{uuid}/{methodcode}/{fd_methodcode}/{annotationcode}")
async def get_file_location_via_methods(uuid: str, methodcode: str, fd_methodcode: str, annotationcode: str,
                                        connection=Depends(api_pool_manager.get_conn)):
    return await get_annotationlink_through_sample_and_methods(connection, uuid, methodcode, fd_methodcode,
                                                               annotationcode)


@router.get("/get_replicas_of_sample{samplingfeaturecode}")
async def get_replicas_of_sample(samplingfeaturecode: str, connection=Depends(api_pool_manager.get_conn)):
    return await get_replicas_uuid_from_samplingfeaturecode(connection, samplingfeaturecode)


@router.get("/get_samplingfeatureuuid_from_samplingfeaturecode{samplingfeaturecode}")
async def get_uuid_of_sample(samplingfeaturecode: str, connection=Depends(api_pool_manager.get_conn)):
    return await get_samplingfeatureuuid_from_samplingfeaturecode(connection, samplingfeaturecode)
