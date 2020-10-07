from fastapi import Depends, APIRouter
from typing import Optional, List
from odm2_postgres_api.utils.api_pool_manager import api_pool_manager
from odm2_postgres_api.schemas import schemas

from odm2_postgres_api.queries.mass_spec_select_queries import find_result_annotationlink, \
    register_site, register_sample, register_replicas, register_output, \
    get_samplingfeatureid_from_samplingfeaturecode, get_samplingfeaturecodes_of_replicas, \
    get_method_annotationjson, get_samplingfeature_annotationjson, get_samplingfeaturecode_from_result_annotationlink

router = APIRouter()


@router.post("/find_result_annotationlink/")
async def post_result_annotationlink(
    data: schemas.MsResultAnnotationLinkQuery, connection=Depends(api_pool_manager.get_conn)) -> Optional[str]:
    return await find_result_annotationlink(connection, data)


@router.get("/get_samplingfeatureid_from_samplingfeaturecode/{samplingfeaturecode}")
async def samplingfeatureid_from_samplingfeaturecode(
    samplingfeaturecode: str, connection=Depends(api_pool_manager.get_conn)) -> Optional[int]:
    return await get_samplingfeatureid_from_samplingfeaturecode(connection, samplingfeaturecode)


@router.get("/get_samplingfeaturecodes_of_replicas/{samplingfeaturecode}")
async def samplingfeaturecodes_of_replicas(samplingfeaturecode: str, connection=Depends(api_pool_manager.get_conn)) \
        -> List[str]:
    return await get_samplingfeaturecodes_of_replicas(connection, samplingfeaturecode)


@router.get("/get_method_annotationjson/{methodcode}")
async def method_annotationjson(methodcode: str, connection=Depends(api_pool_manager.get_conn)) -> Optional[str]:
    return await get_method_annotationjson(connection, methodcode)


@router.get("/get_samplingfeature_annotationjson/{samplingfeaturecode}")
async def samplingfeature_annotationjson(samplingfeaturecode: str, connection=Depends(api_pool_manager.get_conn)) \
        -> List[str]:
    return await get_samplingfeature_annotationjson(connection, samplingfeaturecode)


@router.get("/get_samplingfeaturecode_from_result_annotationlink/{annotationlink}")
async def samplingfeaturecode_from_result_annotationlink(annotationlink: str,
                                                         connection=Depends(api_pool_manager.get_conn)) \
        -> Optional[str]:
    return await get_samplingfeaturecode_from_result_annotationlink(connection, annotationlink)


@router.post("/register_replicas")
async def post_register_replicas(data: schemas.MsCreateReplicas, connection=Depends(api_pool_manager.get_conn)) -> \
        schemas.MsReplicas:
    return await register_replicas(connection, data)


@router.post("/register_sample")
async def post_register_sample(data: schemas.MsCreateSample, connection=Depends(api_pool_manager.get_conn)) -> int:
    return await register_sample(connection, data)


@router.post("/register_site")
async def post_register_site(data: schemas.MsCreateSite, connection=Depends(api_pool_manager.get_conn)) -> int:
    return await register_site(connection, data)


@router.post("/register_output")
async def post_register_output(data: schemas.MsCreateOutput, connection=Depends(api_pool_manager.get_conn)) -> \
        schemas.MsOutput:
    return await register_output(connection, data)
