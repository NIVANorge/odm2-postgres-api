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
    person: People
    affiliation: Affiliations
    external_identifier: PersonExternalIdentifiers


async def create_or_get_user(conn: asyncpg.connection, user_id_header: str) -> StoredPerson:
    user = get_nivaport_user(user_id_header=user_id_header)
    stored_user = await conn.fetchrow("SELECT p.* from odm2.people p inner join odm2.personexternalidentifiers pei "
                                      "on p.personid=pei.personid "
                                      "WHERE pei.personexternalidentifier = $1",
                                      user.id)

    if stored_user:
        return stored_user
    else:
        async with conn.transaction():
            name = full_name_to_split_tuple(user.displayName)
            people_row = await conn.fetchrow(
                "INSERT INTO odm2.people (personfirstname, personmiddlename, personlastname) "
                "VALUES ($1, $2, $3) "
                "RETURNING *", *name)
            person_created = People(**people_row)

            external_identifier_row = await conn.fetchrow(
                "INSERT INTO odm2.personexternalidentifiers "
                "(personid, personexternalidentifier, externalidentifiersystemid) "
                "VALUES ($1, $2, "
                "(SELECT externalidentifiersystemid FROM odm2.externalidentifiersystems "
                "where externalidentifiersystemname=$3)) "
                "RETURNING *",
                people_row["personid"], user.id, 'niva-port'
            )

            # TODO: affiliationstartdate should really be fetched from loginprovider,
            #  see https://github.com/NIVANorge/niva-port/issues/188
            # TODO: add in additional fields, like organization++
            affiliation = await conn.fetchrow(
                "INSERT INTO odm2.affiliations (personid, affiliationstartdate, primaryemail) "
                "VALUES ($1, $2, $3) "
                "RETURNING *",
                person_created.personid, datetime.utcnow(), user.email)

            return StoredPerson(person=person_created, affiliation=affiliation,
                                external_identifier=external_identifier_row)
