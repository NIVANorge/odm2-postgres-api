import logging
import asyncio

import asyncpg
import aiohttp
from xml.etree import ElementTree

CONTROLLED_VOCABULARY_TABLE_NAMES = (
    'cv_actiontype',
    'cv_aggregationstatistic',
    'cv_annotationtype',
    'cv_censorcode',
    'cv_dataqualitytype',
    'cv_datasettype',
    'cv_directivetype',
    'cv_elevationdatum',
    'cv_equipmenttype',
    'cv_medium',
    'cv_methodtype',
    'cv_organizationtype',
    'cv_propertydatatype',
    'cv_qualitycode',
    'cv_relationshiptype',
    'cv_resulttype',
    'cv_samplingfeaturegeotype',
    'cv_samplingfeaturetype',
    'cv_sitetype',
    'cv_spatialoffsettype',
    'cv_speciation',
    'cv_specimentype',
    'cv_status',
    'cv_taxonomicclassifiertype',
    'cv_unitstype',
    'cv_variablename',
    'cv_variabletype',
)

CV_URL = f"http://vocabulary.odm2.org/api/v1/%s/?format=skos"

# XML encodings
dc = "{http://purl.org/dc/elements/1.1/}%s"
rdf = "{http://www.w3.org/1999/02/22-rdf-syntax-ns#}%s"
skos = "{http://www.w3.org/2004/02/skos/core#}%s"
odm2 = "{http://vocabulary.odm2.org/ODM2/ODM2Terms/}%s"


async def fetch(session, url):
    logging.info(url)
    async with session.get(url) as response:
        if response.status != 200:
            response.raise_for_status()
        return await response.text()


async def update_cv_table(session, pool, table_name):
    xml_string = await fetch(session, CV_URL % table_name[3:])
    if table_name not in CONTROLLED_VOCABULARY_TABLE_NAMES:
        raise RuntimeError(f"table_name: '{table_name}' is invalid")
    sql_statement = f"INSERT INTO {table_name} (term, name, definition, category, sourcevocabularyuri) " \
                    f"VALUES ($1, $2, $3, $4, $5)"
    root = ElementTree.fromstring(xml_string)
    terms = []
    async with pool.acquire() as connection:
        for voc in root.findall(rdf % "Description"):
            if voc.attrib[rdf % "about"].split('/')[-1] == table_name[3:] and voc.find(dc % "title") is not None:
                continue
            term = voc.attrib[rdf % "about"].split('/')[-1]
            name = voc.find(skos % "prefLabel").text
            definition = voc.find(skos % "definition").text if \
                voc.find(skos % "definition") is not None else None
            category = voc.find(odm2 % "category").text if voc.find(odm2 % "category") is not None else None
            source_vocabulary_uri = voc.attrib[rdf % "about"]
            try:
                await connection.execute(sql_statement, term, name, definition, category, source_vocabulary_uri)
                logging.debug(f'Loaded into database: {term}')
                terms.append(term)
            except asyncpg.exceptions.UniqueViolationError:
                logging.debug(f'Already in database: {term}')
    logging.info(f'Added new elements to {table_name}: {terms}')
    return terms


async def synchronize_cv_tables(pool, table_names=CONTROLLED_VOCABULARY_TABLE_NAMES):
    async with aiohttp.ClientSession() as session:
        tasks = []
        for table_name in table_names:
            tasks.append(update_cv_table(session, pool, table_name))
        added_term_list = await asyncio.gather(*tasks)
    return {table_name: added_terms for table_name, added_terms in zip(table_names, added_term_list)}
