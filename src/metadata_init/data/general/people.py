from odm2_postgres_api.schemas.schemas import PeopleCreate, AffiliationsCreate, PeopleAffiliationCreate


def unknown_person(org_id: int) -> PeopleAffiliationCreate:
    p = {
        "personfirstname": "Uknown person",
        "personlastname": "NIVA",
        "affiliationstartdate": "2018-12-15",
        "primaryemail": "unknownemployee@niva.no",
        "organizationid": org_id,
        "isprimaryorganizationcontact": "false",
    }

    return PeopleAffiliationCreate(**p)
