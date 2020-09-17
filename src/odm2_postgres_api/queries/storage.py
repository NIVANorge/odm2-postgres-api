from odm2_postgres_api.queries import core_queries
from odm2_postgres_api.queries.core_queries import insert_pydantic_object, find_row, find_unit
from odm2_postgres_api.schemas.schemas import OrganizationsCreate, Organizations, ExternalIdentifierSystemsCreate, \
    ExternalIdentifierSystems, PeopleAffiliation, PeopleAffiliationCreate, AffiliationsCreate, People, PeopleCreate, \
    Affiliations, ProcessingLevelsCreate, ProcessingLevels, ControlledVocabularyCreate, ControlledVocabulary, \
    UnitsCreate, Units, VariablesCreate, Variables, Methods, MethodsCreate, SamplingFeaturesCreate, SamplingFeatures


async def save_organization(connection, organization: OrganizationsCreate) -> Organizations:
    existing_org = await find_row(table="organizations", id_column="organizationcode",
                                  identifier=organization.organizationcode, model=Organizations, conn=connection)

    if existing_org:
        return existing_org
    return await insert_pydantic_object(connection, 'organizations', organization, Organizations)


async def save_external_identifier_system(connection, system: ExternalIdentifierSystemsCreate):
    table = 'externalidentifiersystems'

    existing = await find_row(connection, table, "externalidentifiersystemname",
                              system.externalidentifiersystemname, ExternalIdentifierSystems)
    if existing:
        return existing
    return await insert_pydantic_object(connection, table, system, ExternalIdentifierSystems)


async def save_person(connection, people_extended: PeopleAffiliationCreate) -> PeopleAffiliation:
    """
    Stores people with their affiliation. If affiliation with primaryemail already exists, the affiliation is returned
    """
    aff = await find_row(table="affiliations", id_column="primaryemail",
                         identifier=people_extended.primaryemail, model=Affiliations, conn=connection)

    if aff:
        # TODO: update person?
        existing_person = await find_row(connection, table="people", id_column="personid", identifier=aff.personid,
                                         model=People)
        return PeopleAffiliation(**{**aff.dict(), **existing_person.dict()})

    async with connection.transaction():
        p = await insert_pydantic_object(connection, "people", PeopleCreate(**people_extended.dict()), People)
        affiliation = AffiliationsCreate(**{**people_extended.dict(), "personid": p.personid})
        stored_aff = await insert_pydantic_object(connection, "affiliations", affiliation, Affiliations)
        return PeopleAffiliation(**{**p.dict(), **stored_aff.dict()})


async def post_processing_levels(connection, processing_level: ProcessingLevelsCreate) -> ProcessingLevels:
    existing = await find_row(connection, "processinglevels", "processinglevelcode",
                              processing_level.processinglevelcode,
                              ProcessingLevels)

    if existing:
        return existing
    return await insert_pydantic_object(connection, 'processinglevels', processing_level, ProcessingLevels)


async def save_controlled_vocab(conn, controlled_vocabulary: ControlledVocabularyCreate) -> ControlledVocabulary:
    return await core_queries.create_new_controlled_vocabulary_item(conn, controlled_vocabulary)


async def save_sampling_features(connection, sampling_feature: SamplingFeaturesCreate):
    existing = await find_row(connection, "samplingfeatures", "samplingfeaturecode",
                              sampling_feature.samplingfeaturecode, SamplingFeatures)
    if existing:
        return existing
    return await core_queries.create_sampling_feature(connection, sampling_feature)


async def save_units(connection, unit: UnitsCreate) -> Units:
    existing = await find_unit(connection, unit)
    if existing:
        return existing
    return await insert_pydantic_object(connection, 'units', unit, Units)


async def save_variables(connection, variable: VariablesCreate) -> Variables:
    existing = await find_row(connection, "variables", "variablecode", variable.variablecode, Variables)
    if existing:
        return existing
    return await insert_pydantic_object(connection, 'variables', variable, Variables)


async def save_methods(connection, method: MethodsCreate) -> Methods:
    existing = await find_row(connection, "methods", "methodcode", method.methodcode, Methods)
    if existing:
        return existing
    return await core_queries.insert_method(connection, method)
