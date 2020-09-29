import asyncio
import logging
import os
from datetime import datetime, date
from typing import List, Optional, Dict
from uuid import uuid4

from fastapi import HTTPException
from httpx import AsyncClient
from nivacloud_logging.log_utils import setup_logging, LogContext, generate_trace_id

from odm2_postgres_api.aquamonitor.aquamonitor_api_types import BegroingSampleCargo, BegroingObservationCargo, \
    BegroingSampleCargoCreate, StationCargo, BegroingObservationCargoCreate, MethodCargo, TaxonomyCodeCargo, \
    ProjectCargo, ProjectCargoCreate
from odm2_postgres_api.aquamonitor.aquamonitor_mapping import METHODS_NIVABASE_MAP
from odm2_postgres_api.schemas.schemas import BegroingResultCreate, Directive, SamplingFeatures, Methods, \
    BegroingObservations, BegroingObservationValues, TaxonomicClassifier


async def get_taxonomy_codes(client: AsyncClient, domain_id: str):
    response = await client.get(f"/taxonomy/domains/{domain_id}/codes")
    response.raise_for_status()
    body = response.json()
    return [code for code in body]


async def get_taxonomy(client: AsyncClient, domain_id: str, code: str) -> TaxonomyCodeCargo:
    res = await client.get(f"/query/taxonomy/domains?domain={domain_id}&code={code}")
    handle_aquamonitor_error(res)
    return TaxonomyCodeCargo(**res.json())


async def get_taxonomy_domain_id(client: AsyncClient, domain: str):
    response = await client.get(f"/query/taxonomy/domains?domain={domain}")
    response.raise_for_status()
    # TODO: consider to return the whole response
    return response.json()["Id"]


async def get_begroing_samples(client: AsyncClient, station_id: int, sample_date: datetime) \
        -> List[BegroingSampleCargo]:
    res = await client.get(f"/query/begroing/samples?stationId={station_id}&sampleDate={sample_date}")
    handle_aquamonitor_error(res)
    samples = [BegroingSampleCargo(**s) for s in res.json()]
    return samples


async def get_begroing_observations(client: AsyncClient, sample_id: int, method_id="null") \
        -> List[BegroingObservationCargo]:
    response = await client.get(f"/begroing/samples/{sample_id}/observations?methodId={method_id}")
    response.raise_for_status()
    return [BegroingObservationCargo(**o) for o in response.json()]


async def post_begroing_sample(client: AsyncClient, sample: BegroingSampleCargoCreate) -> BegroingSampleCargo:
    headers = {"Content-Type": "application/json"}

    response = await client.post(f"/begroing/samples", data=sample.json(), headers=headers)  # type: ignore
    response.raise_for_status()
    return BegroingSampleCargo(**response.json())


async def delete_begroing_sample(client: AsyncClient, begroing_sample_id: int):
    response = await client.delete(f"/begroing/samples/{begroing_sample_id}")
    response.raise_for_status()


async def get_project_stations(client: AsyncClient, project_name: str, station_code: str) -> StationCargo:
    res = await client.get(f"/query/Stations?projectName={project_name}&stationCode={station_code}")
    if res.status_code == 404:
        raise HTTPException(422, f"Did not find station={station_code} in Aquamonitor")
    handle_aquamonitor_error(res)
    return StationCargo(**res.json())


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


async def get_or_create_begroing_sample(client, station: StationCargo,
                                        result: BegroingObservations) -> BegroingSampleCargo:
    samples = await get_begroing_samples(client, station.Id, result.date)
    if len(samples) == 1:
        logging.info("Found sample in aquamonitor", extra={"sample_id": samples[0].Id})
        return samples[0]

    if len(samples) > 1:
        # TODO: what do we do if we get more than 1 sample back? throwing exception for now
        raise Exception(
            f"Found {len(samples)} samples in aquamonitor. This is not taken care of, not implemented yet. "
            f"Please contact cloud@niva.no for help")

    body = BegroingSampleCargoCreate(Station=station, SampleDate=result.date)
    sample = await post_begroing_sample(client, body)
    # TODO: deleting all our writes for now to avoid noise. this is not 100% safe as the delete could fail
    await delete_begroing_sample(client, sample.Id)
    return sample


async def update_begroing_observation(client: AsyncClient,
                                      observation: BegroingObservationCargo) -> BegroingObservationCargo:
    sample_id = observation.Sample.Id
    res = await client.put(f"/begroing/samples/{sample_id}/observations/{observation.Id}",
                           data=observation.json(),  # type: ignore
                           headers={"Content-Type": "application/json"})
    handle_aquamonitor_error(res)
    return res.json()


async def delete_begroing_observation(client: AsyncClient, observation: BegroingObservationCargo):
    sample_id = observation.Sample.Id
    res = await client.delete(f"/begroing/samples/{sample_id}/observations/{observation.Id}")
    handle_aquamonitor_error(res)
    return res.json()


async def post_begroing_observation(client: AsyncClient, sample: BegroingSampleCargo,
                                    obs: BegroingObservationValues) -> BegroingObservationCargo:
    method = await get_method_by_id(client, METHODS_NIVABASE_MAP[obs.method.methodname])
    taxon = await get_taxonomy(client, "Begroingsalger", obs.taxon.taxonomicclassifiername)

    body = BegroingObservationCargoCreate(Sample=sample, Method=method, Taxonomy=taxon, Value=obs.value)
    res = await client.post(f"/begroing/samples/{sample.Id}/observations",
                            data=body.json(),  # type: ignore
                            headers={"Content-Type": "application/json"})
    handle_aquamonitor_error(res)
    result = BegroingObservationCargo(**res.json())

    # TODO: Deleting during testing
    await delete_begroing_observation(client, result)
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
    with LogContext(sample_id=sample.Id, station_code=station.Code, station_id=station.Id,
                    project_name=result.project.directivedescription, date=result.date):
        created_observations = await asyncio.gather(
            *[post_begroing_observation(client, sample, o) for o in result.observations])
        observation_ids = [o.Id for o in created_observations]
        logging.info("Successfully stored observations", extra={"observation_ids": observation_ids})
        return {"sample": sample, "observations": created_observations}


# TODO: move this to nivacloud-logging
async def traced_request(request):
    request.headers['Span-Id'] = LogContext.getcontext("span_id") or generate_trace_id()
    trace_id = LogContext.getcontext("trace_id")
    if trace_id:
        request.headers['Trace-Id'] = trace_id


async def request_logger(request):
    logging.info(f"Outgoing request", extra={
        "url": request.url,
        "method": request.method,
        "span_id": request.headers.get("Span-Id"),
    })


async def response_logger(response):
    request = response.request
    logging.info(f"Received response", extra={
        "url": request.url,
        "method": request.method,
        "status": response.status_code,
        "elapsed_microseconds": response.elapsed.microseconds,
        "span_id": request.headers.get("Span-Id"),
    })


request_hooks = {
    "request": [traced_request, request_logger],
    "response": [response_logger],
}


class AquamonitorAPIError(Exception):
    def __init__(self, message: str, url: str, method: str, status_code: int, ) -> None:
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
    if "application/json" in response.headers.get('Content-Type'):
        error = response.json()
        message = error.get("Message").replace("\n", "").replace("\r", "") or error
    else:
        message = response.text.replace("\n", "").replace("\r", "")
    raise AquamonitorAPIError(message=message, url=response.request.url, method=response.request.method,
                              status_code=response.status_code)


async def main(api_url: str, username: str, password: str):
    async with AsyncClient(base_url=api_url, auth=(username, password), event_hooks=request_hooks) as client:
        project = Directive(directiveid=1, directivetypecv="Project",
                            directivedescription="Overvåkning av Glomma, Vorma og Øyeren")
        station = SamplingFeatures(samplingfeatureid=1, samplingfeatureuuid=uuid4(),
                                   samplingfeaturecode="HEDEGL06",  # 'DGL SJU' in begroing
                                   samplingfeaturetypecv="Site",
                                   samplingfeaturegeometrywkt="POINT (10.791888159215542 59.90455564858938)")

        bryophyt = TaxonomicClassifier(**{
            "taxonomicclassifierid": 2,
            "taxonomicclassifiercommonname": "BRYOPHYT, Bryophyta",
            "taxonomicclassifiertypecv": "Biology",
            # TODO: fill in name. is this the right one?
            "taxonomicclassifiername": "BRYOPHYT",
            "taxonomicclassifierdescription": "Bryophyta Bryophyta (Moser) Bryophyta\nrubin_nr:BG091",
        })

        achnanthes = TaxonomicClassifier(**{
            "taxonomicclassifiercommonname": "Achnanthes biasolettiana",
            "taxonomicclassifierdescription": "autor:Grun., autor_ref:None↵ph_opt:None, "
                                              "ph_ref:None↵rubin_kode:ACHN BIA",
            "taxonomicclassifiertypecv": "Biology",
            "taxonomicclassifierid": 1015,
            "taxonomicclassifiername": "ACHN BIA",
        })

        method_pres_abs = Methods(**{
            "methodid": 4,
            "methodname": "Presence/absence",
            "methoddescription": "A person observes if a species is present in the observed area or not.",
            "methodtypecv": "Observation",
            "methodcode": "003"
        })

        observation_values = [
            BegroingObservationValues(taxon=bryophyt, method=method_pres_abs, value="x"),
            BegroingObservationValues(taxon=achnanthes, method=method_pres_abs, value="<1"),
        ]

        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        obs = await post_begroing_observations(client,
                                               BegroingObservations(project=project, station=station, date=today,
                                                                    observations=observation_values))


if __name__ == '__main__':
    username = os.environ["AQUAMONITOR_USER"]
    password = os.environ["AQUAMONITOR_PASSWORD"]
    os.environ["HTTPX_LOG_LEVEL"] = "debug"
    setup_logging(plaintext=True)
    asyncio.run(main(api_url="https://test-aquamonitor.niva.no/AquaServices/api",
                     username=username, password=password))
