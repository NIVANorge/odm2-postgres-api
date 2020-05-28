import logging

import requests

from nivacloud_logging.log_utils import setup_logging


sampling_features = {'sampling_features': {
    "samplingfeatureuuid": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
    "samplingfeaturetypecv": "Ships track",
    "samplingfeaturecode": "some_cool_code",
    "elevation_m": 0,
    "samplingfeaturename": "Oslo-Kiel shiptrack",
    "samplingfeaturedescription": "A line string representing a ships track between Oslo and Kiel",
    "samplingfeaturegeotypecv": "Line string",
    "featuregeometrywkt": "LINESTRING (59.916667 10.733333, 54.323333 10.139444)",
    "elevationdatumcv": "MSL"
}}


initial_data = {'actions': {
    "affiliationid": 1,
    "isactionlead": True,
    "roledescription": "Deployed a specific instrument",
    "actiontypecv": "Instrument deployment",
    "methodcode": "000",
    "begindatetime": "2019-11-18T09:55:05",
    "begindatetimeutcoffset": 0
}, 'data_quality': [{
    "dataqualitytypecv": "Physical limit upper bound",
    "dataqualitycode": "FA_salinity_upper",
    "dataqualityvalue": 41,
    "dataqualityvalueunitsid": 1,
    "dataqualitydescription": "Ferrybox salinity upper bound"
}, {
    "dataqualitytypecv": "Physical limit lower bound",
    "dataqualitycode": "FA_salinity_lower",
    "dataqualityvalue": 2,
    "dataqualityvalueunitsid": 1,
    "dataqualitydescription": "Ferrybox salinity lower bound"
}], 'results': {
    # path in old tsb = FA/ferrybox/CTD/SALINITY
    "samplingfeatureuuid": "3fa85f64-5717-4562-b3fc-2c963f66afa6",  # "Oslo-Kiel shiptrack"
    "actionid": 1,
    "dataqualitycodes": ["FA_salinity_upper"],
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
    "dataqualitycode": "FA_salinity_lower"
}}

# {'track_results': {
#     "resultid": 1,
#     "samplingfeatureid": 1,
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
    "result_uuid": "2030a48e-024d-4f6a-a293-eb673321aaa2",
    "derived_from": 6  # This is result "9f20e09a-9c5f-4a00-933a-80065fc19251" (the previous one)
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
    "result_uuid": "67578fb7-8773-427c-adc5-2c2c39957ab3",
    "derived_from": 8  # This is result "128b534f-34c4-45f5-b3b3-ccdff24b56a8" (the previous one)
}, {
    # path in old tsb = FA/ferrybox/CYANO_FLUORESCENCE/RAW
    # I think the units here should be different (because of raw)
    "unitsid": 4,
    "variableid": 5,
    "begin_datetime": "2010-04-19T09:25:29",
    "result_uuid": "9a787978-448a-42ae-92b7-cc655c22575c"
}, {
    # path in old tsb = FA/ferrybox/CYANO_FLUORESCENCE/ADJUSTED
    "unitsid": 4,
    "variableid": 5,
    "begin_datetime": "2010-04-19T09:25:29",
    "result_uuid": "99a674ad-4dc3-400b-acea-890bf009e2ac",
    "derived_from": 10  # This is result "9a787978-448a-42ae-92b7-cc655c22575c" (the previous one)
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
    if 'derived_from' in new_track_result:
        action_response = post_to_odm2_api('actions', {
            "affiliationid": 1,
            "isactionlead": True,
            "roledescription": "Derived a dataset",
            "actiontypecv": "Derivation",
            "methodcode": "001",  # methodcode = 001: "A method from NIVA for deriving a result from another result"
            "begindatetime": new_track_result["begin_datetime"],
            "begindatetimeutcoffset": 0,
            "equipmentids": [],
            "relatedactions": [(new_track_result["derived_from"], 'Is derived from')]
        })
    else:
        action_response = post_to_odm2_api('actions', {
            "affiliationid": 1,
            "isactionlead": True,
            "roledescription": "Deployed a specific instrument",
            "actiontypecv": "Instrument deployment",
            "methodcode": "000",  # methodcode = 000: "A method from NIVA for deploying instruments"
            "begindatetime": new_track_result["begin_datetime"],
            "begindatetimeutcoffset": 0,
            "equipmentids": []
        })

    result_response = post_to_odm2_api('results', {
        "samplingfeatureuuid": "3fa85f64-5717-4562-b3fc-2c963f66afa6",  # "Oslo-Kiel shiptrack"
        "actionid": action_response["actionid"],
        "resultuuid": new_track_result["result_uuid"],
        "resulttypecv": "Track series coverage",
        "variableid": new_track_result["variableid"],
        "unitsid": new_track_result["unitsid"],
        "processinglevelid": 2,  # processinglevelid = 2: "processinglevelcode": "0.05", "definition": "Automated QC"
        "valuecount": 0,
        "statuscv": "Ongoing",
        "sampledmediumcv": "Liquid aqueous"
    })

    track_result_response = post_to_odm2_api('track_results', {
        "resultid": result_response["resultid"],
        "samplingfeatureid": 1,  # samplingfeatureid = 1: "Oslo-Kiel shiptrack"
        "aggregationstatisticcv": "Continuous",
        "track_result_values": [],
        "track_result_locations": []
    })
    logging.info(f"successfully added trackresult: {new_track_result}, response: {track_result_response}")


def main():
    setup_logging(plaintext=True)
    data_sets = [sampling_features, initial_data]
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
