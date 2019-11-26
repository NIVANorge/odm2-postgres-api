import logging

import requests

from nivacloud_logging.log_utils import setup_logging


people_and_organizations = {"people": {
    "personfirstname": "Roald",
    "personlastname": "Storm"
}, "organizations": {
    "organizationtypecv": "Research institute",
    "organizationcode": "niva-no",
    "organizationname": "NIVA Norsk institut for vannforskning",
    "organizationdescription": "The Norwegian institute for water research",
    "organizationlink": "www.niva.no"
}, "affiliations": {
    "personid": 1,
    "affiliationstartdate": "2018-12-15",
    "primaryemail": "roald.storm@niva.no",
    "organizationid": 1,
    "isprimaryorganizationcontact": "false",
    "primaryphone": "0047 413 60 753"
}}

sampling_features = {'sampling_features': {
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
}}

# processing levels copied from : https://github.com/ODM2/ODM2/blob/master/doc/ODM2Docs/core_processinglevels.md
processing_levels = {'processing_levels': [{
    "processinglevelcode": "0",
    "definition": "Raw Data",
    "explanation": "Raw data is defined as unprocessed data and data products that have not undergone quality "
                   "control. Depending on the data type and data transmission system, raw data may be available "
                   "within seconds or minutes after real-time. Examples include real time precipitation, streamflow "
                   "and water quality measurements."
}, {
    "processinglevelcode": "0.05",
    "definition": "Automated QC",
    "explanation": "Automated procedures have been applied as an initial quality control. These procedures leave the "
                   "data as is but flag obviously erroneous values using a qualitycode"
}, {
    "processinglevelcode": "0.1",
    "definition": "First Pass QC",
    "explanation": "A first quality control pass has been performed to remove out of range and obviously erroneous "
                   "values. These values are either deleted from the record, or, for short durations, values are "
                   "interpolated."
}, {
    "processinglevelcode": "0.2",
    "definition": "Second Pass QC",
    "explanation": "A second quality control pass has been performed to adjust values for instrument drift. Drift "
                   "corrections are performed using a linear drift correction algorithm and field notes designating "
                   "when sensor cleaning and calibration occurred."
}, {
    "processinglevelcode": "1",
    "definition": "Quality Controlled Data",
    "explanation": "Quality controlled data have passed quality assurance procedures such as routine estimation of "
                   "timing and sensor calibration or visual inspection and removal of obvious errors. An example is "
                   "USGS published streamflow records following parsing through USGS quality control procedures."
}, {
    "processinglevelcode": "2",
    "definition": "Derived Products",
    "explanation": "Derived products require scientific and technical interpretation and include multiple-sensor "
                   "data. An example might be basin average precipitation derived from rain gages using an "
                   "interpolation procedure."
}, {
    "processinglevelcode": "3",
    "definition": "Interpreted Products",
    "explanation": "These products require researcher (PI) driven analysis and interpretation, model-based "
                   "interpretation using other data and/or strong prior assumptions. An example is basin average "
                   "precipitation derived from the combination of rain gages and radar return data."
}, {
    "processinglevelcode": "4",
    "definition": "Knowledge Products",
    "explanation": "These products require researcher (PI) driven scientific interpretation and multidisciplinary "
                   "data integration and include model-based interpretation using other data and/or strong prior "
                   "assumptions. An example is percentages of old or new water in a hydrograph inferred from an "
                   "isotope analysis."
}]}

units_and_variables = {'units': [{
    "unitstypecv": "Salinity",
    "unitsabbreviation": "PSU",
    "unitsname": "Practical Salinity Unit"
}, {
    "unitstypecv": "Mass temperature",
    "unitsabbreviation": "degC",
    "unitsname": "temperature"
}, {
    "unitstypecv": "Concentration or density mass per volume",
    "unitsabbreviation": "mg/m^3",
    "unitsname": "milligram per cubic meter"
}, {
    "unitstypecv": "Concentration count per count",
    "unitsabbreviation": "ppb",
    "unitsname": "Parts Per billion"
}], 'variables': [{
    "variabletypecv": "Chemistry",
    "variablenamecv": "Salinity",
    "variabledefinition": "A variable for measuring salinity",
    "variablecode": "001",
    "nodatavalue": -9999
}, {
    "variabletypecv": "Climate",
    "variablenamecv": "Temperature",
    "variabledefinition": "A variable for measuring temperature",
    "variablecode": "002",
    "nodatavalue": -9999
}, {
    "variabletypecv": "Water quality",
    "variablenamecv": "Chlorophyll fluorescence",
    "variabledefinition": "A variable for measuring chla_fluorescence",
    "variablecode": "003",
    "nodatavalue": -9999
}, {
    "variabletypecv": "Water quality",
    "variablenamecv": "Chlorophyll fluorescence",
    "variabledefinition": "A variable for measuring pah_fluorescence",
    "variablecode": "004",
    "nodatavalue": -9999
}, {
    "variabletypecv": "Water quality",
    "variablenamecv": "Chlorophyll fluorescence",
    "variabledefinition": "A variable for measuring cyano_fluorescence",
    "variablecode": "005",
    "nodatavalue": -9999
}, {
    "variabletypecv": "Water quality",
    "variablenamecv": "Chlorophyll fluorescence",
    "variabledefinition": "A variable for measuring cdom_fluorescence",
    "variablecode": "006",
    "nodatavalue": -9999
}]}


initial_data = {'controlled_vocabularies': {
    "term": "trackSeriesCoverage",
    "name": "Track series coverage",
    "definition": "A series of ResultValues for a single Variable, measured with a moving platform or some sort of "
                  "variable location, using a single Method, with specific Units, having a specific ProcessingLevel, "
                  "and measured over time.",
    "category": "Coverage",
    "controlled_vocabulary_table_name": "cv_resulttype"
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
    # path in old tsb = FA/ferrybox/CTD/SALINITY
    "samplingfeatureid": 1,
    "actionid": 1,
    "dataqualityids": [1],
    "resultuuid": "314cd400-14a7-489a-ab97-bce6b11ad068",
    "resulttypecv": "Track series coverage",
    "variableid": 1,
    "unitsid": 1,
    "processinglevelid": 2,
    "valuecount": 0,
    "statuscv": "Ongoing",
    "sampledmediumcv": "Liquid aqueous"
}, 'result_data_quality': {  # Add this data quality manually to verify that endpoint works
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

new_track_results = [{
    # path in old tsb = FA/ferrybox/INLET/OXYGEN/TEMPERATURE
    "unitsid": 2,
    "variableid": 2,
    "begin_datetime": "2010-05-28T10:32:02",
    "result_uuid": "09046f91-1405-4b55-9599-6d9ab99ddab4"
}, {
    # path in old tsb = FA/raw/ferrybox/CTD_TEMPERATURE
    "unitsid": 2,
    "variableid": 2,
    "begin_datetime": "2010-04-19T09:25:29",
    "result_uuid": "6d2fae2d-251c-474d-8a3f-25556cf24ecb"
    # }, {  # monkey temperature is not yet used
    #     # path in old tsb = FA/ferrybox/MONKEY/TEMPERATURE
    #     "unitsid": 2,
    #     "variableid": 2,
    #     "begin_datetime": "unknown",
    #     "result_uuid": "275fda57-30c4-46b9-aa51-6e5671f99b00"
}, {
    # path in old tsb = FA/ferrybox/INLET/TEMPERATURE
    "unitsid": 2,
    "variableid": 2,
    "begin_datetime": "2010-04-19T09:25:29",
    "result_uuid": "720b78cb-3e82-4c4d-9b63-7d1ae1b7afc1"
}, {
    # path in old tsb = FA/ferrybox/OXYGEN/TEMPERATURE
    "unitsid": 2,
    "variableid": 2,
    "begin_datetime": "2010-04-19T09:25:29",
    "result_uuid": "2e91ca67-30d1-4ff3-a8cf-f28973f80290"
}, {
    # path in old tsb = FA/ferrybox/CHLA_FLUORESCENCE/RAW
    # I think the units here should be different (because of raw)
    "unitsid": 3,
    "variableid": 3,
    "begin_datetime": "2010-04-19T09:25:29",
    "result_uuid": "9f20e09a-9c5f-4a00-933a-80065fc19251"
}, {
    # path in old tsb = FA/ferrybox/CHLA_FLUORESCENCE/ADJUSTED
    "unitsid": 3,
    "variableid": 3,
    "begin_datetime": "2010-04-19T09:25:29",
    "result_uuid": "2030a48e-024d-4f6a-a293-eb673321aaa2"
}, {
    # path in old tsb = FA/ferrybox/PAH_FLUORESCENCE/RAW
    # I think the units here should be different (because of raw)
    "unitsid": 4,
    "variableid": 4,
    "begin_datetime": "2017-06-08T08:48:59",
    "result_uuid": "128b534f-34c4-45f5-b3b3-ccdff24b56a8"
}, {
    # path in old tsb = FA/raw/ferrybox/PAH_FLUORESCENCE
    "unitsid": 4,
    "variableid": 4,
    "begin_datetime": "2017-06-08T08:48:59",
    "result_uuid": "67578fb7-8773-427c-adc5-2c2c39957ab3"
}, {
    # path in old tsb = FA/ferrybox/CYANO_FLUORESCENCE/ADJUSTED
    "unitsid": 4,
    "variableid": 5,
    "begin_datetime": "2010-04-19T09:25:29",
    "result_uuid": "99a674ad-4dc3-400b-acea-890bf009e2ac"
}, {
    # path in old tsb = FA/ferrybox/CYANO_FLUORESCENCE/RAW
    # I think the units here should be different (because of raw)
    "unitsid": 4,
    "variableid": 5,
    "begin_datetime": "2010-04-19T09:25:29",
    "result_uuid": "9a787978-448a-42ae-92b7-cc655c22575c"
}, {
    # path in old tsb = FA/ferrybox/CDOM_FLUORESCENCE/ADJUSTED
    "unitsid": 4,
    "variableid": 6,
    "begin_datetime": "2010-04-19T09:25:29",
    "result_uuid": "a10ff360-3b1e-4984-a26f-d3ab460bdb51"
}]


def post_to_odm2_api(endpoint, data):
    response = requests.post('http://localhost:8701/' + endpoint, json=data)
    response.raise_for_status()
    logging.info(f"Succesfully inserted into '{endpoint}': {response.json()}")
    return response.json()


def create_new_fantasy_track_result(new_track_result: dict):
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
        "variableid": new_track_result["variableid"],
        "unitsid": new_track_result["unitsid"],
        "processinglevelid": 2,  # processinglevelid = 2: "processinglevelcode": "0.05", "definition": "Automated QC"
        "valuecount": 0,
        "statuscv": "Ongoing",
        "sampledmediumcv": "Liquid aqueous",
        "dataqualityids": []
    })
    track_result_response = post_to_odm2_api('track_results', {
        "resultid": result_response["resultid"],
        "spatialreferenceid": 1,  # samplingfeatureid = 1: "Oslo-Kiel shiptrack"
        "aggregationstatisticcv": "Continuous",
        "track_result_values": [],
        "track_result_locations": []
    })
    logging.info(f"successfully added trackresult: {new_track_result}, response: {result_response}")


def main():
    setup_logging(plaintext=True)
    data_sets = [people_and_organizations, sampling_features, processing_levels, units_and_variables, initial_data]
    for data_set in data_sets:
        for endpoint, data in data_set.items():
            if type(data) == list:
                for data_item in data:
                    post_to_odm2_api(endpoint, data_item)
            else:
                post_to_odm2_api(endpoint, data)

    for new_track_result in new_track_results:
        create_new_fantasy_track_result(new_track_result)


if __name__ == '__main__':
    main()
