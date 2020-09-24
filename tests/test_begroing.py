import pytest
from integration_test_fixtures import wait_for_db, db_conn
from odm2_postgres_api.routes.begroing_routes import post_indices
from odm2_postgres_api.routes.shared_routes import post_directive, post_sampling_features
from odm2_postgres_api.schemas.schemas import DirectivesCreate, SamplingFeaturesCreate, BegroingIndicesCreate


@pytest.mark.asyncio
@pytest.mark.docker
async def test_post_new_indices(db_conn):
    project = {"directivedescription": "BEGLOS", "directivetypecv": "Project"}

    user_header = "eyJpZCI6IDIyMSwgInVpZCI6ICIxZWQyMDBkMy1mMDlhLTQxNjQtOTExMC1hMWYyNGY4OTliYjMiLCAiZGlzcGxheU5hbWUiOiAiXHUwMGM1Z2UgT2xzZW4iLCAiZW1haWwiOiAiZGV2dXNlckBzb21lZW1haWwuY29tIiwgInByb3ZpZGVyIjogIkRldkxvZ2luIiwgImNyZWF0ZVRpbWUiOiAiMjAyMC0wNC0yMFQxMTo0NToyMS4yNDFaIiwgInVwZGF0ZVRpbWUiOiAiMjAyMC0wNC0yMFQxMTo0NToyMS4yNDFaIiwgInJvbGVzIjogWyJhcHBzOmFkbWluIiwgIm5pdmEiXX0="

    project_response = await post_directive(DirectivesCreate(**project), db_conn)
    assert project_response.directiveid > 0

    sf = {
        "samplingfeatureuuid": "75ef7a47-78b5-4596-9935-62072797357a",
        "samplingfeaturetypecv": "Site",
        "samplingfeaturecode": "BEGROING_STATION"
    }

    created_sampling_feature = await post_sampling_features(SamplingFeaturesCreate(**sf), db_conn)
    assert created_sampling_feature.samplingfeatureid > 0

    index_data = {
        "project_ids": [project_response.directiveid],
        "station_uuid": created_sampling_feature.samplingfeatureuuid,
        "date": "2020-09-01T00:00:00.000Z",
        "indices": [{"indexType": "PIT EQR", "indexValue": "11"}]
    }
    response = await post_indices(BegroingIndicesCreate(**index_data), db_conn, user_header)
    assert response
    # TODO: the endpoint does not really respond with anything worth asserting on
