import logging

import requests

from nivacloud_logging.log_utils import setup_logging


initial_data = {'people': {
    "personfirstname": "Roald",
    "personlastname": "Storm"
}, 'organizations': {
    "organizationtypecv": "Research institute",
    "organizationcode": "niva-no",
    "organizationname": "NIVA Norsk institut for vannforskning",
    "organizationdescription": "The Norwegian institute for water research",
    "organizationlink": "www.niva.no"
}, 'affiliations': {
    "personid": 1,
    "affiliationstartdate": "2018-12-15",
    "primaryemail": "roald.storm@niva.no",
    "organizationid": 1,
    "isprimaryorganizationcontact": False,
    "primaryphone": "0047 413 60 753"
}, 'methods': {
    "methodtypecv": "Unknown",
    "methodcode": "000",
    "methodname": "Unknown Method",
    "methoddescription": "An unknown method from NIVA",
    "organizationid": 1
}, 'actions': {
    "affiliationid": 1,
    "isactionlead": True,
    "roledescription": "Deployed the instrument on a ferrybox",
    "actiontypecv": "Instrument deployment",
    "methodid": 1,
    "begindatetime": "2019-11-18T09:55:05",
    "begindatetimeutcoffset": 0
}, 'sampling_features': {
    "samplingfeatureuuid": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
    "samplingfeaturetypecv": "Ships track",
    "samplingfeaturecode": "string",
    "elevation_m": 0,
    "samplingfeaturename": "Oslo-Kiel shiptrack",
    "samplingfeaturedescription": "A line string representing a shpis track between Oslo and Kiel",
    "samplingfeaturegeotypecv": "Line string",
    "featuregeometrywkt": "LINESTRING (59.916667 10.733333, 54.323333 10.139444)",
    "elevationdatumcv": "MSL"
}, 'processing_levels': {
    "processinglevelcode": "0",
    "definition": "There has been no quality control or processing on this data",
    "explanation": "There has been no quality control or processing on this data"
}, 'units': {
    "unitstypecv": "Salinity",
    "unitsabbreviation": "PSU",
    "unitsname": "Practical Salinity Unit"
}, 'variables': {
    "variabletypecv": "Chemistry",
    "variablecode": "001",
    "variablenamecv": "Salinity",
    "nodatavalue": -9000
}, 'results': {
    "samplingfeatureid": 1,
    "actionid": 1,
    "resultuuid": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
    "resulttypecv": "Time series coverage",
    "variableid": 1,
    "unitsid": 1,
    "processinglevelid": 1,
    "valuecount": 0,
    "statuscv": "Ongoing",
    "sampledmediumcv": "Liquid aqueous"
}}


def main():
    setup_logging(plaintext=True)
    api_url = 'http://localhost:5000/'
    for endpoint, data in initial_data.items():
        response = requests.post(api_url + endpoint, json=data)
        response.raise_for_status()
        logging.info(f"Succesfully inserted into '{endpoint}': {response.json()}")


if __name__ == '__main__':
    main()
