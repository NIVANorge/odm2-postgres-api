import json
import uuid
from typing import Optional, List, Dict
from datetime import datetime
from odm2_postgres_api.queries import core_queries
from odm2_postgres_api.schemas import schemas
import asyncpg


async def find_result_annotationlink(
    conn: asyncpg.connection, data: schemas.MsResultAnnotationLinkQuery
) -> Optional[str]:
    samplingfeaturecode = data.samplingfeaturecode
    annotationcode = data.annotationcode
    results = await conn.fetch(
        "select a.annotationlink, a.annotationjson from annotations a "
        "left join resultannotations ra on a.annotationid = ra.annotationid "
        "left join results r on r.resultid = ra.resultid "
        "left join featureactions fa on fa.featureactionid = r.featureactionid "
        "left join samplingfeatures sf on sf.samplingfeatureid = fa.samplingfeatureid "
        "where sf.samplingfeaturecode = $1 and a.annotationcode = $2 ",
        samplingfeaturecode,
        annotationcode,
    )

    link = None
    if results is None:
        return link
    methods = data.Methods.dict()
    for result in results:
        if not result["annotationjson"]:
            return result["annotationlink"]
        else:
            parameters = json.loads(result["annotationjson"])
            valid = True
            for method_code_type, methodcode in methods.items():
                # methodcode defaults to None because of the Pydantic model
                if methodcode and parameters["Methods"][method_code_type] != methodcode:
                    valid = False
            if valid:
                link = result["annotationlink"]
                break
    return link


async def get_result_annotationlinks_per_replica(conn: asyncpg.connection, samplingfeaturecode: str) -> Optional[List]:
    results = await conn.fetch(
        "select a.annotationlink, a.annotationjson from annotations a "
        "left join resultannotations ra on a.annotationid = ra.annotationid "
        "left join results r on r.resultid = ra.resultid "
        "left join featureactions fa on fa.featureactionid = r.featureactionid "
        "left join samplingfeatures sf on sf.samplingfeatureid = fa.samplingfeatureid "
        "where sf.samplingfeaturecode = $1",
        samplingfeaturecode,
    )
    return results


async def get_samplingfeatureid_from_samplingfeaturecode(
    conn: asyncpg.connection, samplingfeaturecode: str
) -> Optional[int]:
    result = await conn.fetchrow(
        "select samplingfeatureid from samplingfeatures " "where samplingfeaturecode=$1 ",
        samplingfeaturecode,
    )
    return result["samplingfeatureid"] if result is not None else None


async def get_samplingfeaturecodes_of_replicas(conn: asyncpg.connection, samplingfeaturecode: str) -> List[str]:
    result = await conn.fetch(
        "select sf.samplingfeaturecode from samplingfeatures sf "
        "left join relatedfeatures r on sf.samplingfeatureid = r.samplingfeatureid "
        "left join samplingfeatures sf2 on sf2.samplingfeatureid = r.relatedfeatureid "
        "where r.relationshiptypecv = 'Is child of' and sf2.samplingfeaturecode=$1 ",
        samplingfeaturecode,
    )
    return [(record["samplingfeaturecode"]) for record in result]


async def get_method_annotationjson(conn: asyncpg.connection, methodcode: str) -> Optional[str]:
    result = await conn.fetchrow(
        "select annotationjson from annotations a "
        "left join methodannotations ma on ma.annotationid = a.annotationid "
        "left join methods m on ma.methodid = m.methodid "
        "where m.methodcode = $1",
        methodcode,
    )
    return result["annotationjson"] if result is not None else None


async def get_samplingfeature_annotationjson(conn: asyncpg.connection, samplingfeaturecode: str) -> List[str]:
    result = await conn.fetch(
        "select annotationjson, annotationdatetime from annotations a "
        "left join samplingfeatureannotations sa on a.annotationid = sa.annotationid "
        "left join samplingfeatures sf on sf.samplingfeatureid = sa.samplingfeatureid "
        "where samplingfeaturecode = $1",
        samplingfeaturecode,
    )

    result_with_datetime = [item for item in result if item["annotationdatetime"] is not None]
    sorted(result_with_datetime, key=lambda item: item["annotationdatetime"])
    return [(record["annotationjson"]) for record in result_with_datetime]


async def get_samplingfeaturecode_from_result_annotationlink(
    conn: asyncpg.connection, annotationlink: str
) -> Optional[str]:
    result = await conn.fetchrow(
        "select samplingfeaturecode from samplingfeatures s "
        "left join featureactions f on s.samplingfeatureid = f.samplingfeatureid "
        "left join results r on f.featureactionid = r.featureactionid "
        "left join resultannotations ra on r.resultid = ra.resultid "
        "left join annotations a on ra.annotationid = a.annotationid "
        "where annotationlink like $1",
        f"%{annotationlink}%",
    )
    return result["samplingfeaturecode"] if result is not None else None


async def register_replicas(conn: asyncpg.connection, data: schemas.MsCreateReplicas) -> schemas.MsReplicas:
    """
    This function registers mass_spec raw data.
    It first registers water replica (a child of the main water sample) in samplingfeature table
    and the corresponding action of specimen fractionation in the action table.
    Next it registers results of operating mass spectrometer in the result table and
    and the corresponding action of specimen analysis in the action table.
    """
    async with conn.transaction():
        action_time = datetime.now()
        sampling_feature = schemas.SamplingFeaturesCreate(
            samplingfeatureuuid=uuid.uuid4(),
            samplingfeaturecode=data.samplingfeaturecode,
            samplingfeaturetypecv="Specimen",
            relatedsamplingfeatures=[(data.parent_samplingfeatureid, "Is child of")],
            annotations=[
                schemas.AnnotationsCreate(
                    annotationjson=data.samplingfeatureannotationjson,
                    annotationdatetime=action_time,
                    annotationtypecv="Specimen annotation",
                    annotationtext="Processing parameters",
                    annotationutcoffset=0,
                )
            ],
        )

        fractionate_sample_data = schemas.ActionsCreate(
            actiondescription="Fractionate water sample",
            actiontypecv="Specimen fractionation",
            methodcode="mass_spec:fractionate_sample",
            begindatetime=action_time,
            sampling_features=[sampling_feature],
            affiliationid=1,
            isactionlead=True,
            begindatetimeutcoffset=0,
        )
        completed_fractionate_sample = await core_queries.do_action(conn, fractionate_sample_data)
        ran_mass_spec_data = schemas.ActionsCreate(
            actiondescription="Operated mass spectrometer",
            actiontypecv="Specimen analysis",
            methodcode="mass_spec:create_data",
            begindatetime=action_time,
            sampling_features=[],
            affiliationid=1,
            isactionlead=True,
            begindatetimeutcoffset=0,
        )
        completed_ran_mass_spec = await core_queries.do_action(conn, ran_mass_spec_data)
        units_create = schemas.UnitsCreate(unitstypecv="Dimensionless", unitsabbreviation="-", unitsname="")
        units = await core_queries.find_unit(conn, units_create, raise_if_none=True)
        variables = await core_queries.find_row(
            conn,
            "variables",
            "variablecode",
            "mass_spec_00",
            schemas.Variables,
            raise_if_none=True,
        )
        processinglevels = await core_queries.find_row(
            conn,
            "processinglevels",
            "processinglevelcode",
            "0",
            schemas.ProcessingLevels,
            raise_if_none=True,
        )
        resultannotation = schemas.AnnotationsCreate(
            annotationlink=data.resultannotationlink,
            annotationtypecv="Result annotation",
            annotationcode="raw",
            annotationtext="Check link for file location",
        )
        result = schemas.ResultsCreate(
            samplingfeaturecode=sampling_feature.samplingfeaturecode,
            statuscv="Complete",
            variableid=variables.variableid,
            unitsid=units.unitsid,  # type: ignore
            processinglevelid=processinglevels.processinglevelid,
            valuecount=0,
            resulttypecv="Measurement",
            sampledmediumcv="Liquid aqueous",
            annotations=[resultannotation],
            resultdatetime=action_time,
            validdatetime=action_time,
            resultdatetimeutcoffset=0,
            validdatetimeutcoffset=0,
            actionid=completed_ran_mass_spec.actionid,
            resultuuid=uuid.uuid4(),
        )
        completed_result = await core_queries.create_result(conn, result)

    return schemas.MsReplicas(
        fractionate_sample=completed_fractionate_sample,
        ran_mass_spec=completed_ran_mass_spec,
        results=completed_result,
    )


async def register_sample(conn: asyncpg.connection, data: schemas.MsCreateSample) -> int:
    """
    This function registers the main sample collected at a given Site in samplingfeature table.
    """
    async with conn.transaction():
        samplingfeatureid = await get_samplingfeatureid_from_samplingfeaturecode(conn, data.samplingfeaturecode)
        if samplingfeatureid is None:
            relatedsamplingfeatures = []
            if data.parent_samplingfeatureid is not None:
                relatedsamplingfeatures.append((data.parent_samplingfeatureid, "Was collected at"))

            sampling_feature = schemas.SamplingFeaturesCreate(
                samplingfeatureuuid=uuid.uuid4(),
                samplingfeaturecode=data.samplingfeaturecode,
                samplingfeaturetypecv="Specimen",
                relatedsamplingfeatures=relatedsamplingfeatures,
            )

            if data.collection_time is None:
                variable_fields = {"actiondescription": "Registered water sample", "begindatetime": datetime.utcnow()}
            else:
                variable_fields = {
                    "actiondescription": "Collected water sample",
                    "begindatetime": data.collection_time,
                }
            ms_sample_data = schemas.ActionsCreate(
                **variable_fields,
                actiontypecv="Specimen collection",
                methodcode="mass_spec:collect_sample",
                isactionlead=True,
                sampling_features=[sampling_feature],
                affiliationid=1,
                begindatetimeutcoffset=0,
            )

            completed_sample = await core_queries.do_action(conn, ms_sample_data)
            samplingfeatureid = completed_sample.sampling_features[0].samplingfeatureid

    return samplingfeatureid


async def register_output(conn: asyncpg.connection, data: schemas.MsCreateOutput):
    """
    This function registers mass_spec processed data.
    It registers results of derivation with a given methodcode in the result table and
    and the corresponding action of derivation in the action table.
    """
    async with conn.transaction():
        action_time = datetime.now()
        ran_ms_convert = schemas.ActionsCreate(
            actiontypecv="Derivation",
            methodcode=data.methodcode,
            begindatetime=action_time,
            sampling_features=[],
            affiliationid=1,
            isactionlead=True,
            begindatetimeutcoffset=0,
        )

        completed_ran_ms_convert = await core_queries.do_action(conn, ran_ms_convert)
        units_create = schemas.UnitsCreate(unitstypecv="Dimensionless", unitsabbreviation="-", unitsname="")
        units = await core_queries.find_unit(conn, units_create, raise_if_none=True)
        variables = await core_queries.find_row(
            conn,
            "variables",
            "variablecode",
            data.variablecode,
            schemas.Variables,
            raise_if_none=True,
        )
        processinglevels = await core_queries.find_row(
            conn,
            "processinglevels",
            "processinglevelcode",
            "0",
            schemas.ProcessingLevels,
            raise_if_none=True,
        )
        resultannotation = schemas.AnnotationsCreate(
            annotationlink=data.resultannotationlink,
            annotationtypecv="Result annotation",
            annotationcode=data.resultannotationcode,
            annotationtext="Check link for file location",
            annotationjson=data.resultannotationjson,
            annotationdatetime=action_time,
            annotationutcoffset=0,
        )

        result = schemas.ResultsCreate(
            samplingfeaturecode=data.samplingfeaturecode,
            statuscv="Complete",
            variableid=variables.variableid,
            unitsid=units.unitsid,  # type: ignore
            processinglevelid=processinglevels.processinglevelid,
            valuecount=0,
            resulttypecv="Measurement",
            sampledmediumcv="Liquid aqueous",
            annotations=[resultannotation],
            resultdatetime=action_time,
            validdatetime=action_time,
            resultdatetimeutcoffset=0,
            validdatetimeutcoffset=0,
            actionid=completed_ran_ms_convert.actionid,
            resultuuid=uuid.uuid4(),
        )

        completed_result = await core_queries.create_result(conn, result)

    return schemas.MsOutput(action=completed_ran_ms_convert, result=completed_result)
