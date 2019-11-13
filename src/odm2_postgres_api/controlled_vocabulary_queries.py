import logging
import asyncio

import asyncpg
import aiohttp
from xml.etree import ElementTree

VOCABULARIES = [
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
]

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


async def update_all_cv_tables(connection):
    async with aiohttp.ClientSession() as session:
        xml_strings = await asyncio.gather(*[asyncio.create_task(fetch(session, CV_URL % table_name[3:]))
                                             for table_name in VOCABULARIES])
    logging.info(xml_strings)
    for xml_string, table_name in zip(xml_strings, VOCABULARIES):
        await update_cv_table(connection, xml_string, table_name)


async def update_cv_table(connection, xml_string, table_name):
    sql_statement = f"INSERT INTO {table_name} (term, name, definition, category, sourcevocabularyuri) " \
                    f"VALUES ($1, $2, $3, $4, $5)"
    root = ElementTree.fromstring(xml_string)
    tasks = []
    for voc in root.findall(rdf % "Description"):
        if voc.attrib[rdf % "about"].split('/')[-1] == table_name[3:] and voc.find(dc % "title") is not None:
            continue
        tasks.append(asyncio.create_task(insert_term(connection, sql_statement, voc)))

    terms = await asyncio.gather(*tasks)
    terms = [term[0] for term in terms]

    logging.info(f'Added new elements to {table_name}: {terms}')


async def insert_term(connection, sql_statement, voc):
    term = voc.attrib[rdf % "about"].split('/')[-1]
    name = voc.find(skos % "prefLabel").text
    definition = voc.find(skos % "definition").text if \
        voc.find(skos % "definition") is not None else None
    category = voc.find(odm2 % "category").text if voc.find(odm2 % "category") is not None else None
    source_vocabulary_uri = voc.attrib[rdf % "about"]
    try:
        await connection.execute(sql_statement, term, name, definition, category, source_vocabulary_uri)
        logging.debug(f'Loaded into database: {term}')
        return [term]
    except asyncpg.exceptions.UniqueViolationError:
        logging.debug(f'Already in database: {term}')
        return []
