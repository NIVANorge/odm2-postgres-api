import json
import logging
import os
from typing import List

from odm2_postgres_api.controlled_vocabularies.download_cvs import CONTROLLED_VOCABULARY_TABLE_NAMES
from odm2_postgres_api.queries.core_queries import find_row, insert_pydantic_object
from odm2_postgres_api.schemas.schemas import ControlledVocabulary


def read_cv_from_file(cv_name) -> List[ControlledVocabulary]:
    path = os.path.join(os.path.dirname(__file__), "cv_definitions", f"{cv_name}.json")
    logging.info("Loading CV's from file", extra={"file": path})
    with open(path) as f:
        o = json.load(f)
        return [ControlledVocabulary(**cv) for cv in o["objects"]]


async def load_controlled_vocabularies(db_pool):
    logging.info("Loading controlled vocabularies defined as json")
    async with db_pool.acquire() as conn:
        for cv_name in CONTROLLED_VOCABULARY_TABLE_NAMES:
            cvs = read_cv_from_file(cv_name)
            for cv in cvs:
                existing = await find_row(conn, cv_name, "term", cv.term, ControlledVocabulary)
                if not existing:
                    await insert_pydantic_object(conn, cv_name, cv, ControlledVocabulary)
