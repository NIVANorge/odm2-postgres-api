from odm2_postgres_api.schemas.schemas import OrganizationsCreate


def niva_org() -> OrganizationsCreate:
    org = {
        "organizationtypecv": "Research institute",
        "organizationcode": "niva-no",
        "organizationname": "Norsk institutt for vannforskning",
        "organizationdescription": "Norwegian institute for water research",
        "organizationlink": "www.niva.no",
    }
    return OrganizationsCreate(**org)
