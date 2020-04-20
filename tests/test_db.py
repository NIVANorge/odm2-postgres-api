import pytest

from odm2_postgres_api.queries.user import create_or_get_user
from integration_test_fixtures import clear_db, wait_for_db, init_dbpool


@pytest.mark.docker
@pytest.mark.asyncio
async def test_create_user(wait_for_db, clear_db):
    db_pool = await init_dbpool()
    async with db_pool.acquire() as connection:
        # TODO: create obj and base64 encode in test instead of this, as this is not very readable
        created_person = await create_or_get_user(connection,
                                        "eyJpZCI6MjIxLCJ1aWQiOiIxZWQyMDBkMy1mMDlhLTQxNjQtOTExMC1hMWYyNGY4OTliYjMiLCJkaXNwbGF5TmFtZSI6IsOFZ2UgT2xzZW4iLCJlbWFpbCI6ImRldnVzZXJAc29tZWVtYWlsLmNvbSIsInByb3ZpZGVyIjoiRGV2TG9naW4iLCJjcmVhdGVUaW1lIjoiMjAyMC0wNC0yMFQxMTo0NToyMS4yNDFaIiwidXBkYXRlVGltZSI6IjIwMjAtMDQtMjBUMTE6NDU6MjEuMjQxWiIsInJvbGVzIjpbImFwcHM6YWRtaW4iLCJuaXZhIl19")

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
