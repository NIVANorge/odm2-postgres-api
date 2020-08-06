import asyncpg
import logging


async def get_sample_uuid_through_result_annotation_link(conn: asyncpg.connection, annotationlink: str):
    result = await conn.fetchrow('select samplingfeatureuuid from samplingfeatures s '
                                 'left join featureactions f on s.samplingfeatureid = f.samplingfeatureid '
                                 'left join results r on f.featureactionid = r.featureactionid '
                                 'left join resultannotations ra on r.resultid = ra.resultid '
                                 'left join annotations a on ra.annotationid = a.annotationid '
                                 'where annotationlink like $1', f'%{annotationlink}%')
    return result['samplingfeatureuuid']


async def get_sample_annotations_through_sample_uuid(conn: asyncpg.connection, samplingfeatureuuid: str):
    result = await conn.fetchrow('select annotationjson from annotations a '
                                 'left join samplingfeatureannotations sa on a.annotationid = sa.annotationid '
                                 'left join samplingfeatures sf on sf.samplingfeatureid = sa.samplingfeatureid '
                                 'where samplingfeatureuuid = $1', samplingfeatureuuid)
    return result['annotationjson']


async def get_parent_feature_uuid(conn: asyncpg.connection, samplingfeatureuuid: str):
    result = await conn.fetchrow('select sf.samplingfeatureuuid from samplingfeatures sf '
                                 'left join relatedfeatures rf on sf.samplingfeatureid = rf.relatedfeatureid '
                                 'left join samplingfeatures sa on sa.samplingfeatureid = rf.samplingfeatureid '
                                 'where sa.samplingfeatureuuid = $1', samplingfeatureuuid)
    return result['samplingfeatureuuid']


async def get_result_annotationlink_through_sample_uuid(conn: asyncpg.connection, samplingfeatureuuid: str,
                                                        annotationcode: str):
    result = await conn.fetchrow('select distinct annotationlink from annotations a '
                                 'left join resultannotations ra on a.annotationid = ra.annotationid '
                                 'left join results r on r.resultid = ra.resultid '
                                 'left join featureactions fa on fa.featureactionid = r.featureactionid '
                                 'left join samplingfeatures sf on sf.samplingfeatureid = fa.samplingfeatureid '
                                 'where sf.samplingfeatureuuid = $1 and a.annotationcode = $2',
                                 samplingfeatureuuid, annotationcode)
    return result['annotationlink']


async def get_method_annotation_through_method_code(conn: asyncpg.connection, methodcode: str):
    result = await conn.fetchrow('select annotationjson from annotations a '
                                 'left join methodannotations ma on ma.annotationid = a.annotationid '
                                 'left join methods m on ma.methodid = m.methodid '
                                 'where m.methodcode = $1', methodcode)
    return result['annotationjson']
