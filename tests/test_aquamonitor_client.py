import json
import re
from datetime import datetime
from uuid import uuid4

import pytest
import respx
from httpx import AsyncClient

from odm2_postgres_api.aquamonitor.aquamonitor_api_types import (
    StationCargo,
    BegroingSampleCargo,
    MethodCargo,
    TaxonomyCodeCargo,
    DomainCargo,
    TaxonomyCargo,
    BegroingObservationCargo,
    BegroingObservationCargoCreate,
    StationResponse,
)
from odm2_postgres_api.aquamonitor.aquamonitor_client import post_begroing_observations
from odm2_postgres_api.schemas.schemas import (
    BegroingObservations,
    Directive,
    SamplingFeatures,
    TaxonomicClassifier,
    Methods,
    BegroingObservationValues,
)


@pytest.mark.asyncio
@respx.mock
async def test_post_begroing_observations():
    async with AsyncClient(base_url="https://doesnotexist.niva.no") as client:
        project = Directive(directivetypecv="project", directivedescription="project1", directiveid=1)
        station = SamplingFeatures(
            samplingfeatureid=1,
            samplingfeatureuuid=uuid4(),
            samplingfeaturecode="HEDEGL06",  # 'DGL SJU' in begroing
            samplingfeaturetypecv="Site",
            featuregeometrywkt="POINT (10.791888159215542 59.90455564858938)",
        )

        bryophyt = TaxonomicClassifier(
            **{
                "taxonomicclassifierid": 2,
                "taxonomicclassifiercommonname": "BRYOPHYT, Bryophyta",
                "taxonomicclassifiertypecv": "Biology",
                "taxonomicclassifiername": "BRYOPHYT",
                "taxonomicclassifierdescription": "Bryophyta Bryophyta (Moser) Bryophyta\nrubin_nr:BG091",
            }
        )

        achnanthes = TaxonomicClassifier(
            **{
                "taxonomicclassifiercommonname": "Achnanthes biasolettiana",
                "taxonomicclassifierdescription": "autor:Grun., autor_ref:None↵ph_opt:None, "
                "ph_ref:None↵rubin_kode:ACHN BIA",
                "taxonomicclassifiertypecv": "Biology",
                "taxonomicclassifierid": 1015,
                "taxonomicclassifiername": "ACHN BIA",
            }
        )

        method_pres_abs = Methods(
            **{
                "methodid": 4,
                "methodname": "Presence/absence",
                "methoddescription": "A person observes if a species is present in the observed area or not.",
                "methodtypecv": "Observation",
                "methodcode": "003",
            }
        )

        observations = [
            BegroingObservationValues(taxon=bryophyt, method=method_pres_abs, value="x"),
            BegroingObservationValues(taxon=achnanthes, method=method_pres_abs, value="<1"),
        ]

        station_cargo = StationCargo(
            Id=3,
            ProjectId=6,
            Type={},
            Code=station.samplingfeaturecode,
            Name=station.samplingfeaturename,
            Selected=False,
        )
        mock_get_stations = respx.get(
            url=re.compile(r"^.*/Stations\?.*$"),
            status_code=200,
            content=StationResponse(Size=100, Total=1, Records=[station_cargo]).json(),
        )

        sample_date = datetime.now()

        sample = BegroingSampleCargo(Id=567, Station=station_cargo, SampleDate=sample_date)
        mock_get_sample = respx.get(
            url=re.compile(r"^.*/query/begroing/samples\?.*$"),
            status_code=200,
            content=f"[{sample.json()}]",
        )

        method = MethodCargo(Id=25128, Name="Presence/absence", Laboratory="NIVA", MethodRef="BEAFOR")
        mock_get_method_by_id = respx.get(url=re.compile(r"^.*/methods/\d*$"), status_code=200, content=method.json())

        taxon = TaxonomyCodeCargo(
            Id=7587,
            Code="BRYOPHYT",
            Name="Bryophyta",
            Domain=DomainCargo(Id=9),
            Taxonomy=TaxonomyCargo(Id=9),
        )
        mock_get_taxon = respx.get(
            url=re.compile(r"^.*/query/taxonomy/domains?.*$"),
            status_code=200,
            content=taxon.json(),
        )

        mock_post_observations = respx.post(
            url=re.compile(f"^.*/begroing/samples/{sample.Id}/observations?.*$"),
            status_code=200,
            content=BegroingObservationCargo(Id=555, Sample=sample, Method=method, Taxonomy=taxon, Value="x").json(),
        )

        begroing_observations = BegroingObservations(
            project=project,
            date=sample_date,
            station=station,
            observations=observations,
        )

        response = await post_begroing_observations(client, begroing_observations)
        assert mock_post_observations.call_count == len(observations)
        for i, obs in enumerate(observations):
            # check that our outgoing request is wired correctly together
            body = [l for l in mock_post_observations.calls[i][0].stream][0]
            obs_post_body = BegroingObservationCargoCreate(**json.loads(body))
            assert obs_post_body.Method == method
            assert obs_post_body.Taxonomy == taxon
            assert obs_post_body.Value == obs.value
            assert obs_post_body.Sample == sample
