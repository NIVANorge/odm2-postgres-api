from datetime import timezone

import pytest
from odm2_postgres_api.routes.fish_rfid.fish_rfid_routes import post_fish_observation

from integration_test_fixtures import db_conn, wait_for_db
from odm2_postgres_api.queries.core_queries import find_or_create_sampling_feature
from odm2_postgres_api.routes.fish_rfid.fish_rfid_types import (
    FishObservationRequest,
    FishObservationResponse,
    FishObservationCreate,
)
from test_utils import user_header


@pytest.mark.docker
@pytest.mark.asyncio
async def test_create_sampling_feature_if_not_exists(db_conn):
    code = "AAA"
    sf_type = "Specimen"
    fish_sf = await find_or_create_sampling_feature(db_conn, code, sf_type)
    assert fish_sf.samplingfeatureid > 0
    fish_sf2 = await find_or_create_sampling_feature(db_conn, code, sf_type)
    assert fish_sf == fish_sf2


example_file = """
S,2020-06-08 14:03:33.800,G,00:00:00.900,R,0000_000174764644,AAA,10,0.2
S,2020-06-08 14:04:04.100Z,G,00:00:01.500,A,982_000199951389,AAA,16,0.2
S,2020-06-08 16:04:06.600+02:00,G,00:00:01.600,A,982_000356452118,AAA,17,0.2
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
@pytest.mark.asyncio
async def test_store_fish_observations(db_conn):
    observations = [csv_to_observation(l) for l in example_file.split("\n") if l]
    payload = FishObservationRequest(observations=observations)

    station_codes = set([o.station_code for o in observations])
    for station_code in station_codes:
        station = await find_or_create_sampling_feature(
            db_conn,
            station_code,
            "Site",
            wkt="POINT (10.907013757789976 60.25819134332953)",
        )

    response = await post_fish_observation(payload, db_conn, niva_user=user_header()["Niva-User"])

    for i, obs in enumerate(observations):
        stored = response.observations[i]

        if obs.datetime.tzinfo:
            assert obs.datetime.astimezone(timezone.utc) == stored.action.begindatetime.replace(tzinfo=timezone.utc)
        else:
            assert obs.datetime == stored.action.begindatetime
        assert stored.action.begindatetimeutcoffset == 0  # we store all times as UTC
        assert stored.station_sampling_feature in [sf.samplingfeatureuuid for sf in response.station_sampling_features]
        assert stored.fish_sampling_feature in [sf.samplingfeatureuuid for sf in response.fish_sampling_features]

        assert stored.action.enddatetime - stored.action.begindatetime == obs.duration
        assert stored.action.actionid > 0
        assert stored.action.actiontypecv == "Observation"
        assert stored.action.methodcode == "fish_rfid:observe_fish"

    fish_tags = set([o.fish_tag for o in observations])

    assert len(response.fish_sampling_features) == len(fish_tags)
    for fish_sf in response.fish_sampling_features:
        assert fish_sf.samplingfeaturecode in fish_tags
        assert fish_sf.samplingfeatureid > 0
        assert fish_sf.samplingfeatureuuid is not None
        assert fish_sf.samplingfeaturetypecv == "Specimen"

    station_codes = set([o.station_code for o in observations])

    assert len(response.station_sampling_features) == len(station_codes)
    for station_sf in response.station_sampling_features:
        assert station_sf.samplingfeatureuuid is not None
        assert station_sf.samplingfeatureid > 0
        assert station_sf.samplingfeaturecode in station_codes
        assert station_sf.samplingfeaturetypecv == "Site"
        assert station_sf.featuregeometrywkt is not None
