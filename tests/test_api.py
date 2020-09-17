import json

import pytest
from starlette.testclient import TestClient
from integration_test_fixtures import wait_for_db, clear_db

from odm2_postgres_api.app import app
from odm2_postgres_api.schemas.schemas import DirectivesCreate, SamplingFeaturesCreate


@pytest.mark.docker
def test_post_new_indices(wait_for_db, clear_db):
    project = {"directivedescription": "BEGLOS", "directivetypecv": "Project"}

    headers = {
        "Content-Type": "application/json",
        "Niva-User": "eyJpZCI6IDIyMSwgInVpZCI6ICIxZWQyMDBkMy1mMDlhLTQxNjQtOTExMC1hMWYyNGY4OTliYjMiLCAiZGlzcGxheU5hbWUiOiAiXHUwMGM1Z2UgT2xzZW4iLCAiZW1haWwiOiAiZGV2dXNlckBzb21lZW1haWwuY29tIiwgInByb3ZpZGVyIjogIkRldkxvZ2luIiwgImNyZWF0ZVRpbWUiOiAiMjAyMC0wNC0yMFQxMTo0NToyMS4yNDFaIiwgInVwZGF0ZVRpbWUiOiAiMjAyMC0wNC0yMFQxMTo0NToyMS4yNDFaIiwgInJvbGVzIjogWyJhcHBzOmFkbWluIiwgIm5pdmEiXX0="
    }

    with TestClient(app) as client:
        project_response = client.post(url="/directives", data=DirectivesCreate(**project).json())
        assert project_response.status_code == 200

        sf = {
            "samplingfeatureuuid": "75ef7a47-78b5-4596-9935-62072797357a",
            "samplingfeaturetypecv": "Site",
            "samplingfeaturecode": "BEGROING_STATION"
        }
        sampling_feature_res = client.post(url="/sampling_features", data=SamplingFeaturesCreate(**sf).json())
        assert sampling_feature_res.status_code == 200

        index_data = {
            "project_ids": [project_response.json()["directiveid"]],
            "station_uuid": sampling_feature_res.json()["samplingfeatureuuid"],
            "date": "2020-09-01T00:00:00.000Z",
            "indices": [{"indexType": "PIT EQR", "indexValue": "11"}]
        }
        response = client.post(url="/begroing/indices", data=json.dumps(index_data), headers=headers)
        assert response.status_code == 200
