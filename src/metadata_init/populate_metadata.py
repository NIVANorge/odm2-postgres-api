import logging
from typing import List

import requests
from nivacloud_logging.log_utils import setup_logging
from pydantic import BaseModel

from metadata_init.data.begroing.begroing_metadata import begroing_controlled_vocabularies, begroing_variables, \
    begroing_methods
from metadata_init.data.general.controlled_vocabularies import cvs, controlled_vocabularies
from metadata_init.data.general.external_identifier_systems import external_identifier_systems
from metadata_init.data.general.methods import methods
from metadata_init.data.general.organisations import niva_org
from metadata_init.data.general.people import unknown_person
from metadata_init.data.general.processing_levels import general_processing_levels
from metadata_init.data.general.units import units
from metadata_init.data.general.variables import variables
from odm2_postgres_api.schemas.schemas import Organizations, People


def main(api_host: str):
    def post_api(path: str, data: BaseModel):
        response = requests.post(api_host + path, data=data.json())
        response.raise_for_status()
        logging.info(f"Succesfully inserted data': {response.json()}", extra={"path": path, "host": api_host})
        return response.json()

    def batch_post_api(path: str, list: List[BaseModel]):
        return [post_api(path, data) for data in list]

    # trigger CV updates
    # TODO: this takes about ~30 secs, which is a bit slow. May want to do this in another way
    cv_update_res = requests.patch(api_host + "controlled_vocabularies")
    cv_update_res.raise_for_status()

    niva_org_created = Organizations(**post_api("organizations", niva_org()))
    niva_org_id = niva_org_created.organizationid
    batch_post_api("external_identifier_system", external_identifier_systems(niva_org_id))

    post_api("people-extended", unknown_person(org_id=niva_org_id))
    batch_post_api("processing_levels", general_processing_levels())
    batch_post_api("controlled_vocabularies", controlled_vocabularies())
    batch_post_api("units", units())
    batch_post_api("variables", variables())
    batch_post_api("methods", methods(org_id=niva_org_id))

    # begroing
    batch_post_api("controlled_vocabularies", begroing_controlled_vocabularies())
    batch_post_api("variables", begroing_variables())
    batch_post_api("methods", begroing_methods(niva_org_id))


if __name__ == '__main__':
    setup_logging(plaintext=True)
    api_host = "http://localhost:8701/"

    main(api_host)
