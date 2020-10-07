import asyncio
import json
import logging
import os

import aiohttp
from nivacloud_logging.log_utils import setup_logging

CONTROLLED_VOCABULARY_TABLE_NAMES = (
    "cv_actiontype",
    "cv_aggregationstatistic",
    "cv_annotationtype",
    "cv_censorcode",
    "cv_dataqualitytype",
    "cv_datasettype",
    "cv_directivetype",
    "cv_elevationdatum",
    "cv_equipmenttype",
    "cv_medium",
    "cv_methodtype",
    "cv_organizationtype",
    "cv_propertydatatype",
    "cv_qualitycode",
    "cv_relationshiptype",
    "cv_resulttype",
    "cv_samplingfeaturegeotype",
    "cv_samplingfeaturetype",
    "cv_sitetype",
    "cv_spatialoffsettype",
    "cv_speciation",
    "cv_specimentype",
    "cv_status",
    "cv_taxonomicclassifiertype",
    "cv_unitstype",
    "cv_variablename",
    "cv_variabletype",
)


async def fetch(session, url):
    logging.info("Downloading CV", extra={"url": url})
    async with session.get(url) as response:
        if response.status != 200:
            response.raise_for_status()
        return await response.text()


CV_URL = "http://vocabulary.odm2.org/api/v1/%s/?format=json"


async def download_and_save_cv(session, cv_name):
    res = await fetch(session, CV_URL % cv_name[3:])
    cv = json.loads(res)
    path = os.path.join(os.path.dirname(__file__), "cv_definitions", f"{cv_name}.json")
    with open(path, "w") as f:
        json.dump(cv, f, indent=2)
    return res


async def main(table_names=CONTROLLED_VOCABULARY_TABLE_NAMES):
    logging.info("Downloading controlled vocabularies", extra={"url": CV_URL})
    async with aiohttp.ClientSession() as session:
        tasks = [download_and_save_cv(session, cv_name) for cv_name in table_names]
        responses = await asyncio.wait(tasks)
    logging.info("Done downloading controlled vocabularies. If there are any git diffs, check these in")


if __name__ == "__main__":
    setup_logging(plaintext=True)
    asyncio.run(main())
