import json
from base64 import b64encode

import pytest

from odm2_postgres_api.queries.user import create_or_get_user
from integration_test_fixtures import db_conn, wait_for_db


@pytest.mark.docker
@pytest.mark.asyncio
async def test_create_user(db_conn):
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
    # TODO: create obj and base64 encode in test instead of this, as this is not very readable
    person = await create_or_get_user(db_conn, b64encode(json.dumps(user_obj).encode('utf-8')))

    assert person.personid > 0
    assert person.personfirstname == 'Åge'
    assert person.personmiddlename is None
    assert person.personlastname == "Olsen"

    assert person.primaryemail == 'devuser@someemail.com'
    assert person.affiliationid is not None

    fetched_person = await create_or_get_user(db_conn, b64encode(json.dumps(user_obj).encode('utf-8')))
    assert person == fetched_person
