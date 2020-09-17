import json
import logging

from odm2_postgres_api.schemas import schemas
import asyncpg


async def find_result_annotationlink(conn: asyncpg.connection, data: schemas.MsResultAnnotationLinkQuery):
    samplingfeaturecode = data.samplingfeaturecode
    annotationcode = data.annotationcode
    results = await conn.fetch('select a.annotationlink, a.annotationjson from annotations a '
                               'left join resultannotations ra on a.annotationid = ra.annotationid '
                               'left join results r on r.resultid = ra.resultid '
                               'left join featureactions fa on fa.featureactionid = r.featureactionid '
                               'left join samplingfeatures sf on sf.samplingfeatureid = fa.samplingfeatureid '
                               'where sf.samplingfeaturecode = $1 and a.annotationcode = $2 ',
                               samplingfeaturecode, annotationcode)

    link = None
    if results is None:
        return link
    methods = data.Methods.dict()
    for result in results:
        if not result["annotationjson"]:
            return result['annotationlink']
        else:
            parameters = json.loads(result["annotationjson"])
            valid = True
            for method_code_type, methodcode in methods.items():
                # methodcode defaults to None because of the Pydantic model
                if methodcode and parameters["Methods"][method_code_type] != methodcode:
                    valid = False
            if valid:
                link = result['annotationlink']
                break
    return link


async def get_samplingfeatureid_from_samplingfeaturecode(conn: asyncpg.connection, samplingfeaturecode: str):
    result = await conn.fetchrow('select samplingfeatureid from samplingfeatures '
                                 'where samplingfeaturecode=$1 ', samplingfeaturecode)
    return result['samplingfeatureid'] if result is not None else None


async def get_samplingfeaturecodes_of_replicas(conn: asyncpg.connection, samplingfeaturecode: str):
    result = await conn.fetch('select sf.samplingfeaturecode from samplingfeatures sf '
                              'left join relatedfeatures r on sf.samplingfeatureid = r.samplingfeatureid '
                              'left join samplingfeatures sf2 on sf2.samplingfeatureid = r.relatedfeatureid '
                              'where r.relationshiptypecv = \'Is child of\' and sf2.samplingfeaturecode=$1 ',
                              samplingfeaturecode)
    return [(record['samplingfeaturecode']) for record in result]


async def get_method_annotationjson(conn: asyncpg.connection, methodcode: str):
    result = await conn.fetchrow('select annotationjson from annotations a '
                                 'left join methodannotations ma on ma.annotationid = a.annotationid '
                                 'left join methods m on ma.methodid = m.methodid '
                                 'where m.methodcode = $1', methodcode)
    return result['annotationjson'] if result is not None else None


async def get_samplingfeature_annotationjson(conn: asyncpg.connection, samplingfeaturecode: str):
    result = await conn.fetch('select annotationjson, annotationdatetime from annotations a '
                              'left join samplingfeatureannotations sa on a.annotationid = sa.annotationid '
                              'left join samplingfeatures sf on sf.samplingfeatureid = sa.samplingfeatureid '
                              'where samplingfeaturecode = $1', samplingfeaturecode)

    result_with_datetime = [item for item in result if item["annotationdatetime"] is not None]
    sorted(result_with_datetime, key=lambda item: item["annotationdatetime"])
    return [(record['annotationjson']) for record in result_with_datetime]


async def get_samplingfeaturecode_from_result_annotationlink(conn: asyncpg.connection, annotationlink: str):
    result = await conn.fetchrow('select samplingfeaturecode from samplingfeatures s '
                                 'left join featureactions f on s.samplingfeatureid = f.samplingfeatureid '
                                 'left join results r on f.featureactionid = r.featureactionid '
                                 'left join resultannotations ra on r.resultid = ra.resultid '
                                 'left join annotations a on ra.annotationid = a.annotationid '
                                 'where annotationlink like $1', f'%{annotationlink}%')
    return result['samplingfeaturecode'] if result is not None else None
