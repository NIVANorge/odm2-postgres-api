import uuid

import pytest
from integration_test_fixtures import wait_for_db, db_conn

from odm2_postgres_api.queries.storage import save_organization, save_controlled_vocab, save_units
from odm2_postgres_api.schemas.schemas import OrganizationsCreate, ControlledVocabularyCreate, UnitsCreate


@pytest.mark.docker
@pytest.mark.asyncio
async def test_create_organization(db_conn):
    org = {
        "organizationtypecv": "Research institute",
        "organizationcode": str(uuid.uuid4()),  # creating random code to avoid duplicates
        "organizationname": "NIVA Norsk institutt for vannforskning",
        "organizationdescription": "The Norwegian institute for water research",
        "organizationlink": "www.niva.no"
    }

    saved = await save_organization(db_conn, OrganizationsCreate(**org))

    assert saved.organizationid > 0
    assert saved.organizationname == org["organizationname"]
    assert saved.organizationcode == org["organizationcode"]
    assert saved.organizationtypecv == org["organizationtypecv"]
    assert saved.organizationdescription == org["organizationdescription"]


@pytest.mark.docker
@pytest.mark.asyncio
async def test_create_units(db_conn):
    units = [{
        "unitstypecv": "SaltySalinity",
        "unitsabbreviation": "SPSU",
        "unitsname": "Dummy unit"
    }, {
        "unitstypecv": "Strange unit",
        "unitsabbreviation": "unitcode",
        "unitsname": "StrangeUnit"
    }]

    for u in units:
        unit = UnitsCreate(**u)
        controlled_vocabulary = {
            "term": unit.unitstypecv,
            "name": unit.unitstypecv,
            "definition": "Very useful unit, such a tremendous effort put down by this unit",
            "category": "TestUnits",
            "controlled_vocabulary_table_name": "cv_unitstype"
        }
        cv = ControlledVocabularyCreate(**controlled_vocabulary)
        saved_cv = await save_controlled_vocab(db_conn, cv)
        assert cv.term == saved_cv.term
        assert cv.name == saved_cv.name
        assert cv.definition == saved_cv.definition
        assert cv.category == saved_cv.category

        saved_unit = await save_units(db_conn, unit)
        assert saved_unit.unitsid > 0
        assert {**{"unitsid": saved_unit.unitsid}, **unit.dict()} == saved_unit.dict()
