import json
import logging

import requests

from nivacloud_logging.log_utils import setup_logging

metadata = {'sampling_features': {
    "samplingfeatureuuid": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
    "samplingfeaturetypecv": "Site",
    "samplingfeaturecode": "901-3-8",
    "samplingfeaturename": "Dalsvatnet",
    "samplingfeaturedescription": "Station ID: 3275"
}, 'methods': [{
    "methodtypecv": "Specimen collection",
    "methodcode": "collect_ms_sample",
    "methodname": "collect_ms_sample",
    "methoddescription": "Collecting sample in the field",  # nopep8
    # Placing metadata in the corresponding tables in the ODM2
    "organizationid": 1  # organization 1 is NIVA
},  {
    "methodtypecv": "Specimen fractionation",
    "methodcode": "fractionate_ms_sample",
    "methodname": "fractionate_ms_sample",
    "methoddescription": "Create a set of sub-samples",  # nopep8
    # Placing metadata in the corresponding tables in the ODM2
    "organizationid": 1  # organization 1 is NIVA
},
    {
    "methodtypecv": "Specimen analysis",
    "methodcode": "niva_ms_sample",
    "methodname": "ms scan",
    "methoddescription": "",  # nopep8
    # Placing metadata in the corresponding tables in the ODM2
    "organizationid": 1  # organization 1 is NIVA
}, {
    "methodtypecv": "Derivation",
    "methodcode": "niva_ms_convert",
    "methodname": "ms convert",
    "methoddescription": "",  # nopep8
    "organizationid": 1  # organization 1 is NIVA
}, {
    "methodtypecv": "Derivation",
    "methodcode": "niva_ms_analysis_wild_west",
    "methodname": "ms data analysis",
    "methoddescription": "variant 'free' has no json describing parameters on the method. The parameters are instead stored on as a json annotation on the 'result'",  # nopep8
    "organizationid": 1  # organization 1 is NIVA
}, {
    "methodtypecv": "Derivation",
    "methodcode": "niva_ms_analysis_1",
    "methodname": "ms data analysis",
    "methoddescription": "variant 1 is a specific method for using the masspec. The parameters are stored in a json annotation linked to the 'method'",  # nopep8
    "organizationid": 1,  # organization 1 is NIVA
    "annotations": [{
        "annotationtypecv": "Method annotation",
        "annotationtext": "The json field holds the parameters with which this method should be executed",
        "annotationjson": json.dumps({
            "mz_range": [0, 0],
            "n_iter": 15000,
            "n_scan": 30,
            "mz_res": 20000,
            "mz_win": 0.02,
            "adj_r2": 0.85,
            "min_int": 2000,
            "int_var": 5,
            "s2n": 2,
            "min_nscan": 3,
            "peak_interp": 1,
            "id_massbank_version": "16032020",
            "id_corr_tresh": 0.9,
            "id_min_int": 500,
            "id_mz_win_pc": 0.8,
            "id_rt_win_pc": 0.25,
            "id_mode": "NEGATIVE",
            "id_source": "ESI",
            "id_parent": 0,
            "id_feature_wgts": [1, 1, 1, 1, 1, 1, 1]
        })
    }]
}, {
    "methodtypecv": "Derivation",
    "methodcode": "niva_ms_analysis_2",
    "methodname": "ms data analysis",
    "methoddescription": "variant 2 functions like such and so (positive reversed flux capicatiors charged to 3.7GWH!!!)",  # nopep8
    "organizationid": 1  # organization 1 is NIVA
}]}

example_action1 = {'actions': {
    "affiliationid": 1,
    "isactionlead": True,
    "roledescription": "Collected water sample",
    "actiontypecv": "Specimen collection",
    "methodcode": "collect_ms_sample",
    "begindatetime": "2020-04-23T07:00:00",
    "begindatetimeutcoffset": 0,
    'sampling_features': [{
        "samplingfeatureuuid": "2727c572-1731-4295-9b41-74481855b7cc",
        "samplingfeaturetypecv": "Specimen",
        "samplingfeaturecode": "MS01",
        "relatedsamplingfeatures": [(1, "Was collected at")]
    }]
}}

example_action2 = {'actions': {
    "affiliationid": 1,
    "isactionlead": True,
    "roledescription": "Operated mass spectrometer",
    "actiontypecv": "Specimen analysis",
    "methodcode": "niva_ms_sample",
    "begindatetime": "2020-04-23T07:00:00",
    "begindatetimeutcoffset": 0
}, 'results': {
    "samplingfeatureuuid": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
    "actionid": 2,
    "resultuuid": "314cd400-14a7-489a-ab97-bce6b11ad068",
    "resulttypecv": "Measurement",
    "variableid": 1,
    "unitsid": 1,
    "processinglevelid": 1,
    "valuecount": 0,
    "statuscv": "Complete",
    "sampledmediumcv": "Liquid aqueous"
}}


def post_to_odm2_api(endpoint, data):
    response = requests.post('http://localhost:8701/' + endpoint, json=data)
    response.raise_for_status()
    logging.info(f"Succesfully inserted into '{endpoint}': {response.json()}")
    return response.json()


def main():
    setup_logging(plaintext=True)
    data_sets = [metadata, example_action1]
    for data_set in data_sets:
        for endpoint, data in data_set.items():
            if type(data) == list:
                for data_item in data:
                    post_to_odm2_api(endpoint, data_item)
            else:
                post_to_odm2_api(endpoint, data)


if __name__ == '__main__':
    main()
