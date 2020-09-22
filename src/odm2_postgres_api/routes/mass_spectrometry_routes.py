from fastapi import Depends, APIRouter
from odm2_postgres_api.utils.api_pool_manager import api_pool_manager
from odm2_postgres_api.schemas import schemas

from odm2_postgres_api.queries.mass_spec_select_queries import find_result_annotationlink, \
    get_samplingfeatureid_from_samplingfeaturecode, get_samplingfeaturecodes_of_replicas, \
    get_method_annotationjson, get_samplingfeature_annotationjson, get_samplingfeaturecode_from_result_annotationlink

router = APIRouter()


@router.post("/find_result_annotationlink/")
async def post_result_annotationlink(data: schemas.MsResultAnnotationLinkQuery,
                                     connection=Depends(api_pool_manager.get_conn)):
    return await find_result_annotationlink(connection, data)


@router.get("/get_samplingfeatureid_from_samplingfeaturecode/{samplingfeaturecode}")
async def samplingfeatureid_from_samplingfeaturecode(samplingfeaturecode: str,
                                                     connection=Depends(api_pool_manager.get_conn)):
    return await get_samplingfeatureid_from_samplingfeaturecode(connection, samplingfeaturecode)


@router.get("/get_samplingfeaturecodes_of_replicas/{samplingfeaturecode}")
async def samplingfeaturecodes_of_replicas(samplingfeaturecode: str, connection=Depends(api_pool_manager.get_conn)):
    return await get_samplingfeaturecodes_of_replicas(connection, samplingfeaturecode)


@router.get("/get_method_annotationjson/{methodcode}")
async def method_annotationjson(methodcode: str, connection=Depends(api_pool_manager.get_conn)):
    return await get_method_annotationjson(connection, methodcode)


@router.get("/get_samplingfeature_annotationjson/{samplingfeaturecode}")
async def samplingfeature_annotationjson(samplingfeaturecode: str, connection=Depends(api_pool_manager.get_conn)):
    return await get_samplingfeature_annotationjson(connection, samplingfeaturecode)


@router.get("/get_samplingfeaturecode_from_result_annotationlink/{annotationlink}")
async def samplingfeaturecode_from_result_annotationlink(annotationlink: str,
                                                         connection=Depends(api_pool_manager.get_conn)):
    return await get_samplingfeaturecode_from_result_annotationlink(connection, annotationlink)
