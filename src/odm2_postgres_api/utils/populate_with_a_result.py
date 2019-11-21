import logging
import datetime as dt

import requests

from nivacloud_logging.log_utils import setup_logging

initial_data = {'controlled_vocabularies': {
    "term": "trackSeriesCoverage",
    "name": "Track series coverage",
    "definition": "A series of ResultValues for a single Variable, measured with a moving platform or some sort of "
                  "variable location, using a single Method, with specific Units, having a specific ProcessingLevel, "
                  "and measured over time.",
    "category": "Coverage",
    "controlled_vocabulary_table_name": "cv_resulttype"
}, 'people': {
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
}, 'spatial_references': {
    "srsname": "Color Fantasy",
    "srsdescription": "Ferrybox between Oslo and Kiel"
}, 'sites': {
    "samplingfeatureid": 1,
    "sitetypecv": "Ocean",
    "latitude": 57.12,
    "longitude": 10.4363885,
    "spatialreferenceid": 1
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
}, 'data_quality': [{
    "dataqualitytypecv": "Physical limit upper bound",
    "dataqualitycode": "001",
    "dataqualityvalue": 41,
    "dataqualityvalueunitsid": 1,
    "dataqualitydescription": "Ferrybox salinity upper bound"
}, {
    "dataqualitytypecv": "Physical limit lower bound",
    "dataqualitycode": "002",
    "dataqualityvalue": 2,
    "dataqualityvalueunitsid": 1,
    "dataqualitydescription": "Ferrybox salinity lower bound"
}], 'results': {
    "samplingfeatureid": 1,
    "actionid": 1,
    "dataqualityid": 1,
    "resultuuid": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
    "resulttypecv": "Track series coverage",
    "variableid": 1,
    "unitsid": 1,
    "processinglevelid": 1,
    "valuecount": 0,
    "statuscv": "Ongoing",
    "sampledmediumcv": "Liquid aqueous"
}, 'result_data_quality': {
    "resultid": 1,
    "dataqualityid": 2
}, 'track_results': {
    "resultid": 1,
    "spatialreferenceid": 1,
    "aggregationstatisticcv": "Continuous",
    "track_result_values": [
        (dt.datetime(2018, 12, 10).isoformat(), 1.1, 'Good'),
        (dt.datetime(2018, 12, 10, 5).isoformat(), 2.2, 'Good'),
        (dt.datetime(2018, 12, 10, 10).isoformat(), 3.3, 'Good')
    ],
    "track_result_locations": [
        (dt.datetime(2018, 12, 10).isoformat(), 57.12, 10.43, 'Good'),
        (dt.datetime(2018, 12, 10, 5).isoformat(), 57.13, 10.44, 'Good'),
        (dt.datetime(2018, 12, 10, 10).isoformat(), 57.14, 10.45, 'Good')
    ]
}}

# {
#   "resultid": 1,
#   "spatialreferenceid": 1,
#   "aggregationstatisticcv": "Continuous",
#   "track_result_values": [
#     ["2018-12-10T00:00:00", 1.1, "Good"],
#     ["2018-12-10T05:00:00", 2.2, "Good"],
#     ["2018-12-10T10:00:00", 3.3, "Good"]
#   ],
#   "track_result_locations": [
#     ["2018-12-10T00:00:00", 57.12, 10.43, "Good"],
#     ["2018-12-10T05:00:00", 57.13, 10.44, "Good"],
#     ["2018-12-10T10:00:00", 57.14, 10.45, "Good"]
#   ]
# }


def do_post(endpoint, data):
    response = requests.post('http://localhost:5000/' + endpoint, json=data)
    response.raise_for_status()
    logging.info(f"Succesfully inserted into '{endpoint}': {response.json()}")


def main():
    setup_logging(plaintext=True)
    for endpoint, data in initial_data.items():
        if type(data) == list:
            for data_item in data:
                do_post(endpoint, data_item)
        else:
            do_post(endpoint, data)


if __name__ == '__main__':
    main()
