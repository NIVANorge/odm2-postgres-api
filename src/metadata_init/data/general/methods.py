from odm2_postgres_api.schemas.schemas import MethodsCreate


def methods(org_id: int) -> MethodsCreate:
    niva_methods = [
        {
            "methodtypecv": "Instrument deployment",
            "methodcode": "000",
            "methodname": "Deploy an Instrument",
            "methoddescription": "A method for deploying instruments",
            "organizationid": org_id,
        }, {
            "methodtypecv": "Derivation",
            "methodcode": "001",
            "methodname": "Derive an adjusted result from a raw result",
            "methoddescription": "A method for deriving a result from another result",
            "organizationid": org_id,
        },
    ]

    return [MethodsCreate(**m) for m in niva_methods]
