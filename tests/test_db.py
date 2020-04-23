import json
from base64 import b64encode

import pytest

from odm2_postgres_api.queries.user import create_or_get_user
from integration_test_fixtures import clear_db, wait_for_db, init_dbpool


@pytest.mark.docker
@pytest.mark.asyncio
async def test_create_user(wait_for_db, clear_db):
    user_obj = {
        "id": 221,
        "uid": "1ed200d3-f09a-4164-9110-a1f24f899bb3",
        "displayName": "Åge Olsen",
        "email": "devuser@someemail.com",
        "provider": "DevLogin",
        "createTime": "2020-04-20T11: 45: 21.241Z",
        "updateTime": "2020-04-20T11: 45: 21.241Z",
        "roles": ["apps: admin", "niva"]
    }
    db_pool = await init_dbpool()
    async with db_pool.acquire() as connection:
        # TODO: create obj and base64 encode in test instead of this, as this is not very readable
        created_person = await create_or_get_user(connection, b64encode(json.dumps(user_obj).encode('utf-8')))

        person = created_person.person
        affiliation = created_person.affiliation
        external_identifier = created_person.external_identifier

        assert person.personid > 0
        assert person.personfirstname == 'Åge'
        assert person.personmiddlename is None
        assert person.personlastname == "Olsen"

        assert affiliation.personid == person.personid
        assert affiliation.affiliationenddate is None
        # TODO: check that date is today (that's how current impl is, really should be from the joined organization day)
        assert affiliation.affiliationstartdate is not None
        assert affiliation.primaryemail == 'devuser@someemail.com'
        assert affiliation.affiliationid is not None

        # TODO: can we do a better assert here?
        assert external_identifier.bridgeid is not None
        assert external_identifier.personid == person.personid
        assert external_identifier.personexternalidentifier == "221"
        # TODO: This maps to external system id created in fixtures (niva-port)
        assert external_identifier.externalidentifiersystemid is not None
