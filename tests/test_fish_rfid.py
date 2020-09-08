from datetime import datetime

import pytest
from starlette.testclient import TestClient

from integration_test_fixtures import wait_for_db, clear_db, init_dbpool
from odm2_postgres_api.app import app
from odm2_postgres_api.routes.fish_rfid.fish_rfid_repository import (
    find_or_create_sampling_feature,
)
from odm2_postgres_api.routes.fish_rfid.fish_rfid_routes import FishObservationCreate
from odm2_postgres_api.routes.fish_rfid.fish_rfid_types import FishObservationRequest, FishObservationResponse
from test_utils import user_header


@pytest.mark.docker
@pytest.mark.asyncio
async def test_create_sampling_feature_if_not_exists(wait_for_db, clear_db):
    observation = FishObservationCreate(
        fish_tag="fish_1",
        datetime=datetime.now(),
        station_code="AAA",
        duration="00:00:01.800",
        field_strength=0.2,
        consecutive_detections=19,
    )
    db_pool = await init_dbpool()
    async with db_pool.acquire() as conn:
        fish_sf = await find_or_create_sampling_feature(conn, observation)
        assert fish_sf.samplingfeatureid > 0


example_file = """
S,2020-06-08 14:03:33.800,G,00:00:00.900,R,0000_000174764644,AAA,10,0.2
S,2020-06-08 14:04:04.100,G,00:00:01.500,A,982_000199951389,AAA,16,0.2
S,2020-06-08 14:04:06.600,G,00:00:01.600,A,982_000356452118,AAA,17,0.2
S,2020-06-08 14:04:09.200,G,00:00:01.700,A,982_000199951389,AAA,18,0.2
S,2020-06-08 14:04:12.000,G,00:00:02.000,A,982_000356452118,AAA,21,0.2
S,2020-06-08 14:34:24.100,G,00:00:00.800,R,0000_000174764644,AAA,9,0.2
S,2020-06-08 14:51:57.600,G,00:00:01.000,A,982_000356452118,AAA,11,0.2
S,2020-06-08 14:52:01.000,G,00:00:01.200,A,982_000199951389,AAA,13,0.2
S,2020-06-08 14:52:06.200,G,00:00:02.200,A,982_000356452118,AAA,23,0.2
S,2020-06-08 14:52:11.000,G,00:00:02.400,A,982_000199951389,AAA,25,0.2
S,2020-06-08 14:52:16.300,G,00:00:01.800,A,982_000199951389,AAA,19,0.2
S,2020-06-08 14:52:21.100,G,00:00:02.300,A,982_000356452118,AAA,24,0.2
S,2020-06-08 14:52:25.800,G,00:00:03.300,A,982_000199951389,AAA,34,0.2
S,2020-06-08 14:52:27.500,G,00:00:04.100,A,982_000356452118,AAA,42,0.2
S,2020-06-08 14:52:35.300,G,00:00:02.100,A,982_000356452118,AAA,22,0.2
S,2020-06-08 14:52:37.600,G,00:00:01.800,A,982_000199951389,AAA,23,0.2
S,2020-06-08 14:52:39.800,G,00:00:01.500,A,982_000356452118,AAA,16,0.2
S,2020-06-08 14:52:41.900,G,00:00:00.900,A,982_000199951389,AAA,10,0.2
S,2020-06-08 14:52:46.700,G,00:00:01.100,A,982_000199951389,AAA,12,0.2
S,2020-06-08 14:52:48.700,G,00:00:01.100,A,982_000356452118,AAA,16,0.2
S,2020-06-08 14:55:02.400,G,00:00:00.700,R,0000_000174764644,AAA,8,0.2
S,2020-06-08 14:55:06.400,G,00:00:00.900,R,0000_000174764644,AAA,10,0.5
S,2020-06-08 14:55:14.000,G,00:00:00.900,R,0000_000174764644,AAA,10,0.5
S,2020-06-08 14:55:18.600,G,00:00:00.900,R,0000_000174764644,AAA,10,0.5
"""


def csv_to_observation(l: str) -> FishObservationCreate:
    columns = l.split(",")
    return FishObservationCreate(
        datetime=columns[1],
        duration=columns[3],
        fish_tag=columns[5],
        station_code=columns[6],
        consecutive_detections=columns[7],
        field_strength=columns[8],
    )


@pytest.mark.docker
def test_store_fish_observations(wait_for_db, clear_db):
    observations = [csv_to_observation(l) for l in example_file.split("\n") if l]
    payload = FishObservationRequest(observations=observations)

    with TestClient(app) as client:
        response = client.post("/fish-rfid/observation", payload.json(), headers=user_header())
        assert response.status_code == 200
        body = FishObservationResponse(**response.json())

    for i, obs in enumerate(observations):
        stored = body.observations[i]

        assert obs.datetime == stored.action.begindatetime
        assert obs.station_code == stored.station_sampling_feature.samplingfeaturecode

        assert stored.action.enddatetime - stored.action.begindatetime == obs.duration
        assert stored.action.actionid > 0
        assert stored.action.actiontypecv == "Observation"
        assert stored.action.methodcode == "fish_rfid:observe_fish"

        assert stored.station_sampling_feature.featuregeometrywkt is not None
        assert stored.station_sampling_feature.samplingfeatureid > 0
        assert stored.station_sampling_feature.samplingfeaturetypecv == "Site"

        assert stored.fish_sampling_feature.samplingfeaturecode == obs.fish_tag
        assert stored.fish_sampling_feature.samplingfeatureid > 0
        assert stored.fish_sampling_feature.samplingfeaturetypecv == "Specimen"
