import asyncio
import csv
import logging
import os
from datetime import datetime
from typing import List

import asyncpg
from asyncpg import Connection
from nivacloud_logging.log_utils import setup_logging
from pydantic import BaseModel, ValidationError

from odm2_postgres_api.queries.core_queries import insert_pydantic_object
from odm2_postgres_api.queries.user import StoredPerson
from odm2_postgres_api.schemas.schemas import People, PeopleCreate, PersonExternalIdentifiers, AffiliationsCreate, \
    Affiliations, PersonExternalIdentifiersCreate


class User(BaseModel):
    UserPrincipalName: str
    GivenName: str
    Surname: str
    SamAccountName: str


async def main():
    """
    This script imports users from on-premise active directory into ODM2

    The list of user has to be extracted beforehand via powershell on a windows machine on the local niva network.

    See README.md for more details
    """
    db_host = os.environ.get("TIMESCALE_ODM2_SERVICE_HOST", "localhost")
    db_port = os.environ.get("TIMESCALE_ODM2_SERVICE_PORT", "8700")
    db_user = os.environ.get("ODM2_DB_USER", "postgres")
    db_pwd = os.environ.get("ODM2_DB_PASSWORD", "postgres")
    db_name = os.environ.get("ODM2_DB", "niva_odm2")

    conn = await asyncpg.connect(user=db_user, password=db_pwd, host=db_host, port=db_port, database=db_name)

    users = read_users_csv()
    # TODO: Hardcoding external system id to 2 = onprem AD. Should perhaps be fetched dynamically
    await import_users_from_legacy_AD(conn, 2, users)

    await conn.close()


async def import_users_from_legacy_AD(conn: Connection, ext_identifier_sys_id, users: List[User]):
    for user in users:
        async with conn.transaction():
            name = user.GivenName.strip().split(" ")
            middle = name[1] if (len(name) > 1) else None

            row = await conn.fetchrow("SELECT * FROM odm2.affiliations WHERE primaryemail=$1", user.UserPrincipalName)

            people = PeopleCreate(personfirstname=name[0], personmiddlename=middle, personlastname=user.Surname)

            if not row:
                stored_person = await insert_pydantic_object(conn, "odm2.people", people, People)
                await create_affiliations(conn, stored_person, user),
                await create_external_identifier(conn, stored_person, ext_identifier_sys_id, user)
            else:
                logging.info(f"User already exists in db", extra={"user": user})
                ad_reference = await conn.fetchrow(
                    "SELECT * FROM odm2.personexternalidentifiers where personexternalidentifier=$1",
                    user.SamAccountName)
                if not ad_reference:
                    person = People(**{**row, **people.dict()})
                    logging.info(f"Storing SamAccountName for existing user without reference",
                                 extra={
                                     "person": person,
                                     "sam_account_name": user.SamAccountName
                                 })
                    await create_external_identifier(conn, person, ext_identifier_sys_id, user)


# TODO: check if affiliation exists for user?
async def create_affiliations(conn: Connection, person: People, user: User) -> Affiliations:
    aff = AffiliationsCreate(personid=person.personid,
                             affiliationstartdate=datetime.utcnow(),
                             organizationid=1,
                             primaryemail=user.UserPrincipalName)
    return await insert_pydantic_object(conn, "odm2.affiliations", aff, Affiliations)


# TODO: check if externalidentifier already exists for user?
async def create_external_identifier(conn: Connection, stored_person: People, external_system_id: int, user: User) \
        -> PersonExternalIdentifiers:
    external_id = PersonExternalIdentifiersCreate(personid=stored_person.personid,
                                                  externalidentifiersystemid=external_system_id,
                                                  personexternalidentifier=user.SamAccountName)
    return await insert_pydantic_object(conn, "odm2.personexternalidentifiers", external_id, PersonExternalIdentifiers)


def none_for_empty_string(s: str):
    """Input csv reads empty strings as '' instead of None. We'd want to insert None in such cases"""
    if s:
        return s
    else:
        return None


def read_users_csv() -> List[User]:
    with open("emails.csv", "r") as f:
        invalid_users = []
        firstline = f.readline()
        if "#TYPE" not in firstline:
            # added a quick fix to skip the first line of the csv files, but checking if this was really present
            # AD exports had this line which we ignore:
            # #TYPE Selected.Microsoft.ActiveDirectory.Management.ADUser
            raise Exception(f"Expected a dummy first line from AD export, got this instead: {firstline}")
        reader = csv.DictReader(f, delimiter=";")
        users = []
        for u in reader:
            try:
                if "@niva.no" in u["UserPrincipalName"]:
                    users.append(User(**{k: none_for_empty_string(v) for k, v in u.items()}))
                else:
                    logging.info("Ignoring user without @niva.no email", extra=u)
            except ValidationError:
                invalid_users.append(u)
                logging.info("Ignoring user due to lacking firstname or surname", extra=u)
                pass
        return users


if __name__ == '__main__':
    setup_logging(min_level=logging.INFO, plaintext=True)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    result = loop.run_until_complete(main())
