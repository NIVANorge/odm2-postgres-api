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
    "methodtypecv": "Instrument deployment",
    "methodcode": "000",
    "methodname": "Deploy an Instrument",
    "methoddescription": "A method from NIVA for deploying instruments",
    "organizationid": 1
}, 'actions': {
    "affiliationid": 1,
    "isactionlead": True,
    "roledescription": "Deployed a specific instrument",
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
    "samplingfeaturedescription": "A line string representing a ships track between Oslo and Kiel",
    "samplingfeaturegeotypecv": "Line string",
    "featuregeometrywkt": "LINESTRING (59.916667 10.733333, 54.323333 10.139444)",
    "elevationdatumcv": "MSL"
}, 'spatial_references': {
    "srsname": "Color Fantasy",
    "srsdescription": "Cruise ship between Oslo and Kiel, owned by colorline"
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
    "variabledefinition": "A variable for measuring salinity",
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
    "dataqualityids": [1],
    "resultuuid": "314cd400-14a7-489a-ab97-bce6b11ad068",
    "resulttypecv": "Track series coverage",
    "variableid": 1,
    "unitsid": 1,
    "processinglevelid": 1,
    "valuecount": 0,
    "statuscv": "Ongoing",
    "sampledmediumcv": "Liquid aqueous"
}, 'result_data_quality': {  # Add this data quality manually to verify that method works
    "resultid": 1,
    "dataqualityid": 2
}}

# {'track_results': {
#     "resultid": 1,
#     "spatialreferenceid": 1,
#     "aggregationstatisticcv": "Continuous",
#     "track_result_values": [
#         (dt.datetime(2018, 12, 10).isoformat(), 1.1, 'Good'),
#         (dt.datetime(2018, 12, 10, 5).isoformat(), 2.2, 'Good'),
#         (dt.datetime(2018, 12, 10, 15).isoformat(), 3.3, 'Good')
#     ],
#     "track_result_locations": [
#         (dt.datetime(2018, 12, 10).isoformat(), 57.12, 10.43, 'Good'),
#         (dt.datetime(2018, 12, 10, 5).isoformat(), 57.13, 10.44, 'Good'),
#         (dt.datetime(2018, 12, 10, 10).isoformat(), 57.14, 10.45, 'Good')
#     ]
# }}

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

new_track_results = [{
    "units_type_cv": "Mass temperature",
    "units_abbreviation": "degC",
    "units_name": "temperature",
    "variable_type_cv": "Climate",
    "variable_name_cv": "Temperature",
    "variable_definition": "A variable for measuring temperature",
    "begin_datetime": "2010-05-28T10:32:02",
    "result_uuid": "09046f91-1405-4b55-9599-6d9ab99ddab4"
}, {
    "unitsid": 2,
    "variableid": 2,
    "begin_datetime": "2010-04-19T09:25:29",
    "result_uuid": "6d2fae2d-251c-474d-8a3f-25556cf24ecb"
}, {
    "units_type_cv": "Concentration or density mass per volume",
    "units_abbreviation": "mg/m^3",
    "units_name": "milligram per cubic meter",
    "variable_type_cv": "Water quality",
    "variable_name_cv": "Chlorophyll fluorescence",
    "variable_definition": "A variable for measuring chla_fluorescence",
    "begin_datetime": "2010-04-19T09:25:29",
    "result_uuid": "2030a48e-024d-4f6a-a293-eb673321aaa2"
}, {
    "units_type_cv": "Concentration count per count",
    "units_abbreviation": "ppb",
    "units_name": "Parts Per billion",
    "variable_type_cv": "Water quality",
    "variable_name_cv": "Chlorophyll fluorescence",
    "variable_definition": "A variable for measuring pah_fluorescence",
    "begin_datetime": "2017-06-08T08:48:59",
    "result_uuid": "67578fb7-8773-427c-adc5-2c2c39957ab3"
}]


def post_to_odm2_api(endpoint, data):
    response = requests.post('http://localhost:8701/' + endpoint, json=data)
    response.raise_for_status()
    logging.info(f"Succesfully inserted into '{endpoint}': {response.json()}")
    return response.json()


def create_new_fantasy_track_result(new_track_result: dict):
    if "units_type_cv" in new_track_result:
        unit_response = post_to_odm2_api('units', {
            "unitstypecv": new_track_result["units_type_cv"],
            "unitsabbreviation": new_track_result["units_abbreviation"],
            "unitsname": new_track_result["units_name"]
        })
    else:
        unit_response = {'unitsid': new_track_result["unitsid"]}

    if "variable_type_cv" in new_track_result:
        variable_response = post_to_odm2_api('variables', {
            "variabletypecv": new_track_result["variable_type_cv"],
            "variablecode": f"00{unit_response['unitsid']}",
            "variablenamecv": new_track_result["variable_name_cv"],
            "variabledefinition": new_track_result["variable_definition"],
            "nodatavalue": -9000
        })
    else:
        variable_response = {'variableid': new_track_result["variableid"]}

    action_response = post_to_odm2_api('actions', {
        "affiliationid": 1,
        "isactionlead": True,
        "roledescription": "Deployed a specific instrument",
        "actiontypecv": "Instrument deployment",
        "methodid": 1,  # methodid = 1: "A method from NIVA for deploying instruments"
        "begindatetime": new_track_result["begin_datetime"],
        "begindatetimeutcoffset": 0
    })

    result_response = post_to_odm2_api('results', {
        "samplingfeatureid": 1,  # samplingfeatureid = 1: "Oslo-Kiel shiptrack"
        "actionid": action_response["actionid"],
        "resultuuid": new_track_result["result_uuid"],
        "resulttypecv": "Track series coverage",
        "variableid": variable_response['variableid'],
        "unitsid": unit_response['unitsid'],
        "processinglevelid": 1,  # processinglevelid = 1: "No quality control or processing on this data"
        "valuecount": 0,
        "statuscv": "Ongoing",
        "sampledmediumcv": "Liquid aqueous",
        "dataqualityids": []
    })
    logging.info(f"successfully added trackresult: {new_track_result}, response: {result_response}")


def main():
    setup_logging(plaintext=True)
    for endpoint, data in initial_data.items():
        if type(data) == list:
            for data_item in data:
                post_to_odm2_api(endpoint, data_item)
        else:
            post_to_odm2_api(endpoint, data)

    for new_track_result in new_track_results:
        create_new_fantasy_track_result(new_track_result)


if __name__ == '__main__':
    main()
