import asyncio
import logging
import os
from datetime import datetime
from typing import List, Dict

from fastapi import HTTPException
from httpx import AsyncClient
from nivacloud_logging.log_utils import LogContext, generate_trace_id

from odm2_postgres_api.aquamonitor.aquamonitor_api_types import (
    BegroingSampleCargo,
    BegroingObservationCargo,
    BegroingSampleCargoCreate,
    StationCargo,
    BegroingObservationCargoCreate,
    MethodCargo,
    TaxonomyCodeCargo,
    ProjectCargo,
)
from odm2_postgres_api.aquamonitor.aquamonitor_mapping import METHODS_NIVABASE_MAP
from odm2_postgres_api.schemas.schemas import (
    Directive,
    BegroingObservations,
    BegroingObservationValues,
)


async def get_taxonomy_codes(client: AsyncClient, domain_id: str) -> List[TaxonomyCodeCargo]:
    response = await client.get(f"/taxonomy/domains/{domain_id}/codes")
    handle_aquamonitor_error(response)
    return [TaxonomyCodeCargo(**code) for code in response.json()]


async def get_taxonomy(client: AsyncClient, domain_name: str, code: str) -> TaxonomyCodeCargo:
    res = await client.get(f"/query/taxonomy/domains?domain={domain_name}&code={code}")
    handle_aquamonitor_error(res)
    return TaxonomyCodeCargo(**res.json())


async def get_taxonomy_domain_id(client: AsyncClient, domain: str):
    response = await client.get(f"/query/taxonomy/domains?domain={domain}")
    handle_aquamonitor_error(response)
    # TODO: consider to return the whole response
    return response.json()["Id"]


async def get_begroing_samples(
    client: AsyncClient, station_id: int, sample_date: datetime
) -> List[BegroingSampleCargo]:
    res = await client.get(f"/query/begroing/samples?stationId={station_id}&sampleDate={sample_date}")
    handle_aquamonitor_error(res)
    samples = [BegroingSampleCargo(**s) for s in res.json()]
    return samples


async def get_begroing_observations(
    client: AsyncClient, sample_id: int, method_id="null"
) -> List[BegroingObservationCargo]:
    response = await client.get(f"/begroing/samples/{sample_id}/observations?methodId={method_id}")
    handle_aquamonitor_error(response)
    return [BegroingObservationCargo(**o) for o in response.json()]


async def post_begroing_sample(client: AsyncClient, sample: BegroingSampleCargoCreate) -> BegroingSampleCargo:
    headers = {"Content-Type": "application/json"}

    response = await client.post(f"/begroing/samples", data=sample.json(), headers=headers)  # type: ignore
    handle_aquamonitor_error(response)

    return BegroingSampleCargo(**response.json())


async def delete_begroing_sample(client: AsyncClient, begroing_sample_id: int):
    response = await client.delete(f"/begroing/samples/{begroing_sample_id}")
    handle_aquamonitor_error(response)


async def get_project_stations(client: AsyncClient, project_name: str, station_code: str) -> StationCargo:
    res = await client.get(f"/query/Stations?projectName={project_name}&stationCode={station_code}")
    if res.status_code == 404:
        raise HTTPException(404, f"Did not find station={station_code} in Aquamonitor")
    handle_aquamonitor_error(res)
    body = res.json()
    return StationCargo(**body["Records"][0])


async def get_project(client: AsyncClient, project: Directive) -> ProjectCargo:
    res = await client.get(f"/query/projects")
    handle_aquamonitor_error(res)
    projects = [ProjectCargo(**p) for p in res.json()]

    actual_project = [p for p in projects if p.name == project.directivedescription]
    # TODO: this is very ad hoc. Need to validate that we find a project
    # TODO: should we store reference to project id in odm2 to query more efficiently ?
    return actual_project[0]


async def get_method(client: AsyncClient, method_name: str, unit: str, laboratory: str):
    res = await client.get(f"/query/Methods?name={method_name}&unit={unit}&laboratory={laboratory}&methodRef=")
    handle_aquamonitor_error(res)
    return res.json()


async def get_method_by_id(client: AsyncClient, method_id: int) -> MethodCargo:
    res = await client.get(f"/methods/{method_id}")
    handle_aquamonitor_error(res)
    return MethodCargo(**res.json())


async def methods_for_project_station(client: AsyncClient, project_id: int, station_id: int) -> List[MethodCargo]:
    res = await client.get(f"/projects/{project_id}/stations/{station_id}/methods")
    handle_aquamonitor_error(res)
    return [MethodCargo(**m) for m in res.json()]


async def get_or_create_begroing_sample(
    client, station: StationCargo, result: BegroingObservations
) -> BegroingSampleCargo:
    samples = await get_begroing_samples(client, station.Id, result.date)
    if len(samples) == 1:
        logging.info("Found sample in aquamonitor", extra={"sample_id": samples[0].Id})
        return samples[0]

    if len(samples) > 1:
        # TODO: what do we do if we get more than 1 sample back? throwing exception for now
        raise Exception(
            f"Found {len(samples)} samples in aquamonitor. This is not taken care of, not implemented yet. "
            f"Please contact cloud@niva.no for help"
        )

    body = BegroingSampleCargoCreate(Station=station, SampleDate=result.date)
    sample = await post_begroing_sample(client, body)

    return sample


async def update_begroing_observation(
    client: AsyncClient, observation: BegroingObservationCargo
) -> BegroingObservationCargo:
    sample_id = observation.Sample.Id
    res = await client.put(
        f"/begroing/samples/{sample_id}/observations/{observation.Id}",
        data=observation.json(),  # type: ignore
        headers={"Content-Type": "application/json"},
    )
    handle_aquamonitor_error(res)
    return res.json()


async def delete_begroing_observation(client: AsyncClient, observation: BegroingObservationCargo):
    sample_id = observation.Sample.Id
    res = await client.delete(f"/begroing/samples/{sample_id}/observations/{observation.Id}")
    handle_aquamonitor_error(res)
    return res.json()


async def post_begroing_observation(
    client: AsyncClient, sample: BegroingSampleCargo, obs: BegroingObservationValues
) -> BegroingObservationCargo:
    method = await get_method_by_id(client, METHODS_NIVABASE_MAP[obs.method.methodname])
    taxon = await get_taxonomy(client, "Begroingsalger", obs.taxon.taxonomicclassifiername)

    body = BegroingObservationCargoCreate(Sample=sample, Method=method, Taxonomy=taxon, Value=obs.value)
    res = await client.post(
        f"/begroing/samples/{sample.Id}/observations",
        data=body.json(),  # type: ignore
        headers={"Content-Type": "application/json"},
    )
    handle_aquamonitor_error(res)
    result = BegroingObservationCargo(**res.json())

    return result


async def store_begroing_results(result: BegroingObservations) -> Dict:
    base_url = "https://test-aquamonitor.niva.no/AquaServices/api"
    username = os.environ["AQUAMONITOR_USER"]
    password = os.environ["AQUAMONITOR_PASSWORD"]
    async with AsyncClient(base_url=base_url, auth=(username, password), event_hooks=request_hooks) as client:
        return await post_begroing_observations(client=client, result=result)


async def post_begroing_observations(client: AsyncClient, result: BegroingObservations) -> Dict:
    station_code = result.station.samplingfeaturecode
    station: StationCargo = await get_project_stations(client, result.project.directivedescription, station_code)

    sample = await get_or_create_begroing_sample(client, station, result)
    with LogContext(
        sample_id=sample.Id,
        station_code=station.Code,
        station_id=station.Id,
        project_name=result.project.directivedescription,
        date=result.date,
    ):
        created_observations = await asyncio.gather(
            *[post_begroing_observation(client, sample, o) for o in result.observations]
        )
        observation_ids = [o.Id for o in created_observations]

        logging.info(
            "Successfully stored observations",
            extra={"observation_ids": observation_ids},
        )
        return {"sample": sample, "observations": created_observations}


# TODO: move this to nivacloud-logging
async def traced_request(request):
    request.headers["Span-Id"] = LogContext.getcontext("span_id") or generate_trace_id()
    trace_id = LogContext.getcontext("trace_id")
    if trace_id:
        request.headers["Trace-Id"] = trace_id


async def request_logger(request):
    logging.info(
        f"Outgoing request",
        extra={
            "url": request.url,
            "method": request.method,
            "span_id": request.headers.get("Span-Id"),
        },
    )


async def response_logger(response):
    request = response.request
    logging.info(
        f"Received response",
        extra={
            "url": request.url,
            "method": request.method,
            "status": response.status_code,
            "elapsed_microseconds": response.elapsed.microseconds,
            "span_id": request.headers.get("Span-Id"),
        },
    )


request_hooks = {
    "request": [traced_request, request_logger],
    "response": [response_logger],
}


class AquamonitorAPIError(Exception):
    def __init__(
        self,
        message: str,
        url: str,
        method: str,
        status_code: int,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.url = url
        self.method = method
        self.status_code = status_code

    def __str__(self):
        return f"Aquamonitor API error: {self.message}"


def handle_aquamonitor_error(response):
    if response.status_code < 399:
        return
    if "application/json" in response.headers.get("Content-Type"):
        error = response.json()
        message = error.get("Message").replace("\n", "").replace("\r", "") or error
    else:
        message = response.text.replace("\n", "").replace("\r", "")
    raise AquamonitorAPIError(
        message=message,
        url=response.request.url,
        method=response.request.method,
        status_code=response.status_code,
    )
