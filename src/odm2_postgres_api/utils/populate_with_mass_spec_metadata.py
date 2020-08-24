import json
import logging
import uuid
from urllib.parse import urljoin

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
    "methodcode": "collect_sample",
    "methodname": "collect_sample",
    "methoddescription": "Collecting sample in the field",  # nopep8
    # Placing metadata in the corresponding tables in the ODM2
    "organizationid": 1  # organization 1 is NIVA
}, {
    "methodtypecv": "Specimen fractionation",
    "methodcode": "fractionate_sample",
    "methodname": "fractionate_sample",
    "methoddescription": "Create a set of sub-samples",  # nopep8
    # Placing metadata in the corresponding tables in the ODM2
    "organizationid": 1  # organization 1 is NIVA
},
    {
        "methodtypecv": "Specimen analysis",
        "methodcode": "create_ms_data",
        "methodname": "ms run",
        "methoddescription": "Running mass spectrometer",  # nopep8
        # Placing metadata in the corresponding tables in the ODM2
        "organizationid": 1  # organization 1 is NIVA
    }, {
        "methodtypecv": "Derivation",
        "methodcode": "convert_ms_data",
        "methodname": "ms convert",
        "methoddescription": "",  # nopep8
        "organizationid": 1  # organization 1 is NIVA
    }, {
        "methodtypecv": "Derivation",
        "methodcode": "fd_nointerp_nscan40_allmz",
        "methodname": "feature detection",
        "methoddescription": "Detects features in raw data.  Parameters are "
                             "stored as json annotation on this method. In particular no"
                             "interpolation is perfomred prior to fitting the data, retention "
                             "window is set to 40 and full mass range is used.",
        "organizationid": 1,  # organization 1 is NIVA
        "annotations": [{
            "annotationtypecv": "Method annotation",
            "annotationtext": "The json field holds the parameters with which this method will be executed",
            "annotationjson": json.dumps({
                "mz_range": [0, 0],
                "n_iter": 15000,
                "n_scan": 40,
                "mz_res": 20000,
                "mz_win": 0.02,
                "adj_r2": 0.85,
                "min_int": 2000,
                "int_var": 5,
                "s2n": 2,
                "min_nscan": 3,
                "peak_interp": 0,
            })
        }]
    }, {
        "methodtypecv": "Derivation",
        "methodcode": "fd_nointerp_nscan40_allmz_test",
        "methodname": "feature detection",
        "methoddescription": "Detects features in raw data. This is a test version of method fd_nointerp_nscan40_allmz"
                             "where number of iteration is limited to 25. Parameters are "
                             "stored as json annotation on this method. In particular no"
                             "interpolation is perfomred prior to fitting the data, retention "
                             "window is set to 40 and full mass range is used.",
        "organizationid": 1,  # organization 1 is NIVA
        "annotations": [{
            "annotationtypecv": "Method annotation",
            "annotationtext": "The json field holds the parameters with which this method will be executed",
            "annotationjson": json.dumps({
                "mz_range": [0, 0],
                "n_iter": 25,
                "n_scan": 40,
                "mz_res": 20000,
                "mz_win": 0.02,
                "adj_r2": 0.85,
                "min_int": 2000,
                "int_var": 5,
                "s2n": 2,
                "min_nscan": 3,
                "peak_interp": 0,
            })
        }]
    }, {
        "methodtypecv": "Derivation",
        "methodcode": "fid_positive_16032020",
        "methodname": "feature identification",
        "methoddescription": "Identifies features previously detected using a fd_* and fdc methods. "
                             "Raw data were obtained with positive ESI. Mass bank version 16032020 is "
                             "used for identification."
                             "Additional parameters are stored stored as annotation to this method,",
        "organizationid": 1,  # organization 1 is NIVA
        "annotations": [{
            "annotationtypecv": "Method annotation",
            "annotationtext": "The json field holds the parameters with which this method will be executed",
            "annotationjson": json.dumps({
                "id_massbank_version": "16032020",
                "id_mode": "POSITIVE",
                "id_source": "ESI",
                "id_parent": 0,
                "id_massbank": "MassBankJulia.jld",
                "id_feature_wgts": [1, 1, 1, 1, 1, 1, 1]
            })
        }]
    }, {
        "methodtypecv": "Derivation",
        "methodcode": "fdc",
        "methodname": "feature deconvolution",
        "methoddescription": "Find fragments for peaks detected with fd_* method."
                             "Parameters are stored stored as annotation to this method,",
        "organizationid": 1,  # organization 1 is NIVA
        "annotations": [{
            "annotationtypecv": "Method annotation",
            "annotationtext": "The json field holds the parameters with which this method will be executed",
            "annotationjson": json.dumps({
                "mz_range": [0, 0],
                "id_corr_tresh": 0.9,
                "id_min_int": 500,
                "id_mz_win_pc": 0.8,
                "id_rt_win_pc": 0.25
            })
        }]
    },
]}
#
# do_collect_sample = {'actions': {
#     "affiliationid": 1,
#     "isactionlead": True,
#     "roledescription": "Collected water sample",
#     "actiontypecv": "Specimen collection",
#     "methodcode": "collect_sample",
#     "begindatetime": "2020-04-23T07:00:00",
#     "begindatetimeutcoffset": 0,
#     'sampling_features': [{
#         "samplingfeatureuuid": "2727c572-1731-4295-9b41-74481855b7cc",
#         "samplingfeaturetypecv": "Specimen",
#         "samplingfeaturecode": "MS01",
#         "relatedsamplingfeatures": [(1, "Was collected at")]
#     }]
# }}
# do_fractionate_sample = {'actions': {
#     "affiliationid": 1,
#     "isactionlead": True,
#     "roledescription": "Collected water sample",
#     "actiontypecv": "Specimen fractionation",
#     "methodcode": "fractionate_sample",
#     "begindatetime": "2020-04-23T07:00:00",
#     "begindatetimeutcoffset": 0,
#     'sampling_features': [{
#         "samplingfeatureuuid": '6902dea2-1a4a-4359-9e65-14d6c052c93f',
#         "samplingfeaturetypecv": "Specimen",
#         "samplingfeaturecode": "MS02",
#         "relatedsamplingfeatures": [(1, "Was collected at")]
#     }, {
#         "samplingfeatureuuid": '9d0a0a33-a42f-4368-8158-344188d3a71b',
#         "samplingfeaturetypecv": "Specimen",
#         "samplingfeaturecode": "MS03",
#         "relatedsamplingfeatures": [(1, "Was collected at")]
#     }]}}
#
# do_create_ms_data = [
#     {'actions': {
#     "affiliationid": 1,
#     "isactionlead": True,
#     "roledescription": "Operated mass spectrometer",
#     "actiontypecv": "Specimen analysis",
#     "methodcode": "create_ms_data",
#     "begindatetime": "2020-04-23T07:00:00",
#     "begindatetimeutcoffset": 0
#     }},
#     {'result': {
#         "samplingfeatureuuid": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
#         "actionid": ,
#         "resultuuid": "314cd400-14a7-489a-ab97-bce6b11ad068",
#         "resulttypecv": "Measurement",
#         "variableid": 1,
#         "unitsid": 1,
#         "processinglevelid": 1,
#         "valuecount": 0,
#         "statuscv": "Complete",
#         "sampledmediumcv": "Equipment"
#     }}}]


def try_insert():
    action_data = {"affiliationid": 1,
                   "isactionlead": True,
                   "actiondescription": "Operated mass spectrometer",
                   "actiontypecv": "Specimen analysis",
                   "methodcode": "create_ms_data",
                   "begindatetime": "2020-04-23T07:00:00",
                   "begindatetimeutcoffset": 0}
    action_response = post_to_odm2_api('actions', action_data)
    result_data = {
        "samplingfeatureuuid": '26825151-0076-46b6-8e1a-7d0bba7b0bdb',
        "actionid": action_response['actionid'],
        "resultuuid": str(uuid.uuid4()),
        "resulttypecv": "Measurement",
        "variableid": 1,
        "unitsid": 1,
        "processinglevelid": 1,
        "valuecount": 0,
        "statuscv": "Complete",
        "sampledmediumcv": "Equipment"
    }
    post_to_odm2_api('results', result_data)


def post_to_odm2_api(endpoint, data):
    response = requests.post('http://localhost:8701/' + endpoint, json=data)
    response.raise_for_status()
    logging.info(f"Succesfully inserted into '{endpoint}': {response.json()}")
    return response.json()


def get_from_odm2_api(endpoint):
    response = requests.get('http://localhost:8701/' + endpoint)
    response.raise_for_status()
    logging.info(f"Succesfully got data")
    return response


def try_get():
    # "http://localhost:8701/samplingfeature_uuid_through_result_annotation_link/%252020_02_21_JBA_1000Lakes_HLB_POS_079.rawtar.bz2"
    # link = "%25"+
    link = "2020_02_21_JBA_1000Lakes_HLB_POS_079.raw.tar.bz2"
    data = get_from_odm2_api(f"samplingfeature_uuid_through_result_annotation_link" + link)
    print(data.json())


def main():
    setup_logging(plaintext=True)
    data_sets = [metadata]
    for data_set in data_sets:
        for endpoint, data in data_set.items():
            if type(data) == list:
                for data_item in data:
                    post_to_odm2_api(endpoint, data_item)
            else:
                post_to_odm2_api(endpoint, data)


if __name__ == '__main__':
    main()
    # try_insert()
    # try_get()
