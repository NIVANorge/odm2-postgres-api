import os
import uuid
from collections import defaultdict
from datetime import datetime
from distutils.util import strtobool
from typing import List, cast

from fastapi import Depends, Header, APIRouter
from fastapi.responses import PlainTextResponse

from odm2_postgres_api.aquamonitor.aquamonitor_client import store_begroing_results
from odm2_postgres_api.queries.begroing_queries import find_begroing_results
from odm2_postgres_api.queries.core_queries import find_row, find_unit
from odm2_postgres_api.routes.shared_routes import (
    post_actions,
    post_results,
    post_categorical_results,
    post_measurement_results,
)
from odm2_postgres_api.schemas.schemas import (
    ProcessingLevels,
    UnitsCreate,
    Variables,
    BegroingIndices,
    BegroingObservations,
    BegroingObservationValues,
    TaxonomicClassifier,
    Units,
    BegroingResult,
    BegroingObservation,
)

from odm2_postgres_api.utils.api_pool_manager import api_pool_manager

from odm2_postgres_api.queries.user import create_or_get_user
from odm2_postgres_api.schemas import schemas
from odm2_postgres_api.utils.csv_utils import to_csv

INDEX_NAME_TO_VARIABLE_ID = {
    "PIT": 11,
    "AIP": 12,
    "HBI": 13,
    "HBI2": 14,
    "PIT EQR": 15,
    "AIP EQR": 16,
    "HBI EQR": 17,
    "HBI2 EQR": 18,
    "PIT nEQR": 19,
    "AIP nEQR": 20,
    "HBI nEQR": 21,
    "HBI2 nEQR": 22,
}

router = APIRouter()


@router.get(
    "/station/{sampling_feature_uuid}/project/{project_id}/start/{start_time}/end/{end_time}",
    response_model=List[BegroingObservation],
)
async def get_begroing_results(
    sampling_feature_uuid: uuid.UUID,
    project_id: int,
    start_time: datetime,
    end_time: datetime,
    connection=Depends(api_pool_manager.get_conn),
    niva_user=Header(None),
) -> List[BegroingObservation]:
    user = await create_or_get_user(connection, niva_user)
    return await find_begroing_results(
        connection, project_id, sampling_feature_uuid, start_time=start_time, end_time=end_time
    )


@router.get(
    "/station/{sampling_feature_uuid}/project/{project_id}/start/{start_time}/end/{end_time}/csv",
    response_class=PlainTextResponse,
)
async def get_begroing_results_csv(
    sampling_feature_uuid: uuid.UUID,
    project_id: int,
    start_time: datetime,
    end_time: datetime,
    connection=Depends(api_pool_manager.get_conn),
    niva_user: str = Header(None),
) -> str:
    user = await create_or_get_user(connection, niva_user)
    results = await find_begroing_results(
        connection, project_id, sampling_feature_uuid, start_time=start_time, end_time=end_time
    )

    return to_csv(results)


@router.post("/begroing_result", response_model=schemas.BegroingResult)
async def post_begroing_result(
    begroing_result: schemas.BegroingResultCreate,
    connection=Depends(api_pool_manager.get_conn),
    niva_user: str = Header(None),
):
    user = await create_or_get_user(connection, niva_user)

    observations_per_method = defaultdict(list)
    for index, species in enumerate(begroing_result.taxons):
        used_method_indices = [i for i, e in enumerate(begroing_result.observations[index]) if e]
        if len(used_method_indices) != 1:
            raise ValueError("Must have one and only one method per species")
        observations_per_method[used_method_indices[0]].append(index)

    unit_micr_abundance = await find_unit(
        connection,
        UnitsCreate(
            unitstypecv="Dimensionless",
            unitsabbreviation="-",
            unitsname="Presence or Absence",
        ),
        raise_if_none=True,
    )
    unit_macro_coverage = await find_unit(
        connection,
        UnitsCreate(unitstypecv="Dimensionless", unitsabbreviation="%", unitsname="Percent"),
        raise_if_none=True,
    )

    seconds_unit = await find_unit(
        connection,
        UnitsCreate(unitstypecv="Time", unitsabbreviation="s", unitsname="second"),
        raise_if_none=True,
    )

    result_type_and_unit_dict = {
        "Microscopic abundance": (
            "Category observation",
            unit_micr_abundance.unitsid,
            "Liquid aqueous",
        ),
        "Macroscopic coverage": (
            "Measurement",
            unit_macro_coverage.unitsid,
            "Vegetation",
        ),
    }

    async with connection.transaction():
        for method_index, method_observations in observations_per_method.items():
            method = begroing_result.methods[method_index]

            data_action = schemas.ActionsCreate(
                affiliationid=user.affiliationid,
                isactionlead=True,
                methodcode=method.methodcode,
                actiontypecv=method.methodtypecv,  # This only works when the type is both an action and a method
                begindatetime=begroing_result.date,
                begindatetimeutcoffset=0,
                equipmentids=[],
                directiveids=[e.directiveid for e in begroing_result.projects],
            )

            processing_level = await find_row(
                connection,
                "processinglevels",
                "processinglevelcode",
                "0",
                ProcessingLevels,
            )
            abundance_variable = await find_row(connection, "variables", "variablenamecv", "Abundance", Variables)

            completed_action = await post_actions(data_action, connection)
            for result_index in method_observations:
                data_result = schemas.ResultsCreate(
                    samplingfeatureuuid=begroing_result.station.samplingfeatureuuid,
                    actionid=completed_action.actionid,
                    resultuuid=str(uuid.uuid4()),
                    resulttypecv=result_type_and_unit_dict[method.methodname][0],
                    variableid=abundance_variable.variableid,
                    resultdatetime=begroing_result.date,
                    resultdatetimeutcoffset=0,
                    unitsid=result_type_and_unit_dict[method.methodname][1],
                    taxonomicclassifierid=begroing_result.taxons[result_index]["taxonomicclassifierid"],
                    processinglevelid=processing_level.processinglevelid,
                    valuecount=0,
                    statuscv="Complete",
                    sampledmediumcv=result_type_and_unit_dict[method.methodname][2],
                    dataqualitycodes=[],
                )
                completed_result = await post_results(data_result, connection)
                if method.methodname == "Microscopic abundance":
                    data_categorical_result = schemas.CategoricalResultsCreate(
                        resultid=completed_result.resultid,
                        qualitycodecv="None",
                        datavalue=begroing_result.observations[result_index][method_index],
                        valuedatetime=begroing_result.date,
                        valuedatetimeutcoffset=0,
                    )
                    await post_categorical_results(data_categorical_result, connection)
                else:
                    if begroing_result.observations[result_index][method_index][0] == "<":
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
                        timeaggregationintervalunitsid=seconds_unit.unitsid,
                        datavalue=data_value,
                        valuedatetime=begroing_result.date,
                        valuedatetimeutcoffset=0,
                    )
                    await post_measurement_results(data_measurement_result, connection)
        # TODO: assuming that we have only one project. T*his should also be changed in API endpoint
        observations: List[BegroingObservationValues] = []
        for method_index, method_observations in observations_per_method.items():
            for result_index in method_observations:
                taxon = begroing_result.taxons[result_index]
                value = begroing_result.observations[result_index][method_index]
                values = BegroingObservationValues(
                    taxon=TaxonomicClassifier(**taxon),
                    method=begroing_result.methods[method_index],
                    value=value,
                )
                observations.append(values)

        mapped = BegroingObservations(
            project=begroing_result.projects[0],
            date=begroing_result.date,
            station=begroing_result.station,
            observations=observations,
        )

        if strtobool(os.environ.get("WRITE_TO_AQUAMONITOR", "false")):
            await store_begroing_results(mapped)

    # TODO: Send email about new bucket_files

    return schemas.BegroingResult(personid=user.personid, **begroing_result.dict())


@router.post("/indices", response_model=schemas.BegroingIndices)
async def post_indices(
    new_index: schemas.BegroingIndicesCreate,
    connection=Depends(api_pool_manager.get_conn),
    niva_user: str = Header(None),
) -> BegroingIndices:
    user = await create_or_get_user(connection, niva_user)

    data_action = schemas.ActionsCreate(
        affiliationid=user.affiliationid,
        isactionlead=True,
        methodcode="begroing_6",  # code begroing_6 = Begroing Index Calculation
        actiontypecv="Derivation",
        begindatetime=new_index.date,
        begindatetimeutcoffset=0,
        equipmentids=[],
        directiveids=new_index.project_ids,
    )

    completed_action = await post_actions(data_action, connection)

    processing_level = await find_row(connection, "processinglevels", "processinglevelcode", "0", ProcessingLevels)
    dimensionless_unit = await find_unit(
        connection,
        UnitsCreate(
            unitstypecv="Dimensionless",
            unitsabbreviation="PrsAbs",
            unitsname="Presence or Absence",
        ),
        raise_if_none=True,
    )
    seconds_unit = await find_unit(
        connection,
        UnitsCreate(unitstypecv="Time", unitsabbreviation="s", unitsname="second"),
        raise_if_none=True,
    )
    for index_instance in new_index.indices:
        variable = await find_row(
            connection,
            "variables",
            "variablenamecv",
            index_instance.indexType,
            Variables,
        )

        data_result = schemas.ResultsCreate(
            samplingfeatureuuid=new_index.station_uuid,
            actionid=completed_action.actionid,
            resultuuid=uuid.uuid4(),
            resulttypecv="Measurement",
            variableid=variable.variableid,
            unitsid=dimensionless_unit.unitsid,
            processinglevelid=processing_level.processinglevelid,
            valuecount=1,
            statuscv="Complete",
            sampledmediumcv="Organism",
            dataqualitycodes=[],
        )

        completed_result = await post_results(data_result, connection)

        data_measurement_result = schemas.MeasurementResultsCreate(
            resultid=completed_result.resultid,
            censorcodecv="Not censored",
            qualitycodecv="None",
            aggregationstatisticcv="Unknown",
            timeaggregationinterval=0,
            timeaggregationintervalunitsid=seconds_unit.unitsid,
            datavalue=index_instance.indexValue,
            valuedatetime=new_index.date,
            valuedatetimeutcoffset=0,
        )

        await post_measurement_results(data_measurement_result, connection)

    return schemas.BegroingIndices(personid=user.personid, **new_index.dict())
