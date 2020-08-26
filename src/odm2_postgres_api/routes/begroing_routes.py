import uuid
from collections import defaultdict

from fastapi import Depends, Header, APIRouter
from odm2_postgres_api.routes.shared_routes import post_actions, post_results, post_categorical_results, \
    post_measurement_results

from odm2_postgres_api.utils.api_pool_manager import api_pool_manager

from odm2_postgres_api.queries.user import create_or_get_user
from odm2_postgres_api.schemas import schemas
from odm2_postgres_api.utils import google_cloud_utils


router = APIRouter()


@router.post("/begroing_result", response_model=schemas.BegroingResult)
async def post_begroing_result(begroing_result: schemas.BegroingResultCreate,
                               connection=Depends(api_pool_manager.get_conn),
                               niva_user: str = Header(None)):
    user = await create_or_get_user(connection, niva_user)

    csv_data = google_cloud_utils.generate_csv_from_form(begroing_result)
    if not csv_data:
        return schemas.BegroingResult(personid=user.personid, **begroing_result.dict())
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
                affiliationid=user.affiliationid,
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

    return schemas.BegroingResult(personid=user.personid, **begroing_result.dict())
