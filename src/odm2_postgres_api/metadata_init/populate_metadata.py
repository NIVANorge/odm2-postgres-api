import logging

from odm2_postgres_api.controlled_vocabularies.load_cvs import (
    load_controlled_vocabularies,
)
from odm2_postgres_api.metadata_init.data.begroing.begroing_metadata import (
    begroing_controlled_vocabularies,
    begroing_variables,
    begroing_methods,
)
from odm2_postgres_api.metadata_init.data.fish_rfid.fish_rfid_metadata import (
    fish_rfid_sampling_features,
    fish_rfid_methods,
)
from odm2_postgres_api.metadata_init.data.mass_spec.mass_spec_metadata import (
    mass_spec_methods,
    mass_spec_sampling_features,
    mass_spec_variables,
    mass_spec_controlled_vocabularies,
    mass_spec_annotations,
)
from odm2_postgres_api.metadata_init.data.general.controlled_vocabularies import (
    controlled_vocabularies,
)
from odm2_postgres_api.metadata_init.data.general.external_identifier_systems import (
    external_identifier_systems,
)
from odm2_postgres_api.metadata_init.data.general.methods import methods
from odm2_postgres_api.metadata_init.data.general.organisations import niva_org
from odm2_postgres_api.metadata_init.data.general.people import unknown_person
from odm2_postgres_api.metadata_init.data.general.processing_levels import (
    general_processing_levels,
)
from odm2_postgres_api.metadata_init.data.general.units import units
from odm2_postgres_api.metadata_init.data.general.variables import variables
from odm2_postgres_api.queries.storage import (
    save_organization,
    save_external_identifier_system,
    save_person,
    post_processing_levels,
    save_controlled_vocab,
    save_units,
    save_variables,
    save_methods,
    save_sampling_features,
    save_annotations,
)


async def populate_metadata(db_pool):
    logging.info("Populating ODM2 metadata")
    await load_controlled_vocabularies(db_pool)

    async with db_pool.acquire() as conn:
        niva_org_created = await save_organization(conn, niva_org())
        niva_org_id = niva_org_created.organizationid

        # general static metadata
        for s in external_identifier_systems(niva_org_id):
            await save_external_identifier_system(conn, s)

        await save_person(conn, unknown_person(org_id=niva_org_id))
        for p in general_processing_levels():
            await post_processing_levels(conn, p)

        for cv in controlled_vocabularies():
            await save_controlled_vocab(conn, cv)

        for u in units():
            await save_units(conn, u)

        for v in variables():
            await save_variables(conn, v)

        for m in methods(org_id=niva_org_id):
            await save_methods(conn, m)

        # begroing metadata
        for cv in begroing_controlled_vocabularies():
            await save_controlled_vocab(conn, cv)

        for v in begroing_variables():
            await save_variables(conn, v)

        for m in begroing_methods(org_id=niva_org_id):
            await save_methods(conn, m)

        # masspec metadata
        for sf in mass_spec_sampling_features():
            await save_sampling_features(conn, sf)

        for cv in mass_spec_controlled_vocabularies():
            await save_controlled_vocab(conn, cv)

        for m in mass_spec_methods(org_id=niva_org_id):
            await save_methods(conn, m)

        for v in mass_spec_variables():
            await save_variables(conn, v)

        for a in mass_spec_annotations():
            await save_annotations(conn, a)

        # fish_rfid metadata
        for sf in fish_rfid_sampling_features():
            await save_sampling_features(conn, sf)

        for m in fish_rfid_methods(niva_org_id):
            await save_methods(conn, m)

    logging.info("ODM2 metadata init done")
