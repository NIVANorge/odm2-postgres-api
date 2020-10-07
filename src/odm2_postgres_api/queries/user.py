import json
from base64 import b64decode
from datetime import datetime
from typing import List, Tuple, Optional

import asyncpg
from pydantic import BaseModel

from odm2_postgres_api.schemas.schemas import People, Affiliations, PersonExternalIdentifiers


class NivaPortUser(BaseModel):
    """
    User object passed from niva-port as a base64 encoded json string
    """
    id: str
    email: str
    roles: List[str]
    displayName: str


def full_name_to_split_tuple(fullname: str) -> Tuple[str, Optional[str], str]:
    split = fullname.strip().split(" ")
    if len(split) < 2:
        raise ValueError(f"Could not split name into firstname/lastname")
    head, *mid, tail = split
    middle_name = " ".join(mid) if mid else None
    name_tuple = (head, middle_name, tail)
    return name_tuple


def get_nivaport_user(user_id_header: str) -> NivaPortUser:
    user_json = json.loads(b64decode(user_id_header))
    return NivaPortUser(**user_json)


class StoredPerson(BaseModel):
    personid: int
    personfirstname: str
    personmiddlename: Optional[str]
    personlastname: str
    primaryemail: str
    affiliationid: int


async def create_or_get_user(conn: asyncpg.connection, user_id_header: str) -> StoredPerson:
    """
    Retrieves stored users based on their email as unique identifier

    If user is not found in people/affiliations tables, a new row is inserted

    TODO: handle name updates, see issue https://github.com/NIVANorge/odm2-postgres-api/issues/44
    """
    user = get_nivaport_user(user_id_header=user_id_header)
    people_row = await conn.fetchrow(
        "SELECT p.*, a.affiliationid, a.primaryemail from odm2.people p "
        "inner join odm2.affiliations a "
        "on p.personid=a.personid WHERE a.primaryemail = $1", user.email)

    if not people_row:
        async with conn.transaction():
            name = full_name_to_split_tuple(user.displayName)
            person = await conn.fetchrow(
                "INSERT INTO odm2.people (personfirstname, personmiddlename, personlastname) "
                "VALUES ($1, $2, $3) "
                "RETURNING *", *name)

            # TODO: affiliationstartdate should really be fetched from loginprovider,
            #  see https://github.com/NIVANorge/niva-port/issues/188
            # TODO: add in additional fields, like organization++
            affiliation = await conn.fetchrow(
                "INSERT INTO odm2.affiliations (personid, affiliationstartdate, primaryemail) "
                "VALUES ($1, $2, $3) "
                "RETURNING *", person["personid"], datetime.utcnow(), user.email)
            return StoredPerson(**{**person, **affiliation})
    return StoredPerson(**people_row)
