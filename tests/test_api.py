import json

import pytest
from starlette.testclient import TestClient
from integration_test_fixtures import wait_for_db, clear_db

from odm2_postgres_api.app import app


@pytest.mark.docker
def test_create_organization(wait_for_db, clear_db):
    org = {
        "organizationtypecv": "Research institute",
        "organizationcode": "niva-no",
        "organizationname": "NIVA Norsk institutt for vannforskning",
        "organizationdescription": "The Norwegian institute for water research",
        "organizationlink": "www.niva.no"
    }

    with TestClient(app) as client:
        response = client.post("/organizations", json.dumps(org))
        assert response.status_code == 200


@pytest.mark.docker
def test_create_units(wait_for_db, clear_db):
    units = [{
        "unitstypecv": "Salinity",
        "unitsabbreviation": "PSU",
        "unitsname": "practical salinity unit"
    }, {
        "unitstypecv": "Mass temperature",
        "unitsabbreviation": "degC",
        "unitsname": "temperature"
    }, {
        "unitstypecv": "Time",
        "unitsabbreviation": "s",
        "unitsname": "second"
    }]

    with TestClient(app) as client:

        for unit in units:
            controlled_vocabulary = {
                "term": unit["unitstypecv"],
                "name": unit["unitstypecv"],
                "definition": "Very useful unit, such a tremendous effort put down by this unit",
                "category": "TestUnits",
                "controlled_vocabulary_table_name": "cv_unitstype"
            }
            cv_response = client.post("/controlled_vocabularies", json.dumps(controlled_vocabulary))
            assert cv_response.status_code == 200
            assert cv_response.json() == controlled_vocabulary

            response = client.post("/units", json.dumps(unit))
            assert response.status_code == 200
            body = response.json()
            assert body["unitsid"] is not None
            for key, val in unit.items():
                assert body[key] == val
