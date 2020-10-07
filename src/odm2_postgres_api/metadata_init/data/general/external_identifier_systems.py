from typing import List

from odm2_postgres_api.schemas.schemas import ExternalIdentifierSystemsCreate


def external_identifier_systems(org_id: int) -> List[ExternalIdentifierSystemsCreate]:
    systems = [
        {
            "externalidentifiersystemname": "onprem-active-directory",
            "identifiersystemorganizationid": org_id,
            "externalidentifiersystemdescription": "reference to old 3-letter active "
            "directory usernames (SamAccountName)",
            "externalidentifiersystemurl": "",
        }
    ]

    return [ExternalIdentifierSystemsCreate(**s) for s in systems]
