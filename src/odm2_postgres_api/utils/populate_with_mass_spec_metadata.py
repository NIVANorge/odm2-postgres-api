import logging

import requests

from nivacloud_logging.log_utils import setup_logging

sampling_features = {'sampling_features': {
    "samplingfeatureuuid": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
    "samplingfeaturetypecv": "Specimen",
    "samplingfeaturecode": "another_cool_code",
}}

initial_data = {'actions': {
    "affiliationid": 1,
    "isactionlead": True,
    "roledescription": "Operated mass spectrometer",
    "actiontypecv": "Specimen analysis",
    "methodid": 1,
    "begindatetime": "2020-04-23T07:00:00",
    "begindatetimeutcoffset": 0
}, 'results': {
    "samplingfeatureuuid": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
    "actionid": 1,
    "resultuuid": "314cd400-14a7-489a-ab97-bce6b11ad068",
    "resulttypecv": "Measurement",
    "variableid": 1,
    "unitsid": 1,
    "processinglevelid": 0,
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
    data_sets = [sampling_features]
    for data_set in data_sets:
        for endpoint, data in data_set.items():
            if type(data) == list:
                for data_item in data:
                    post_to_odm2_api(endpoint, data_item)
            else:
                post_to_odm2_api(endpoint, data)


if __name__ == '__main__':
    main()
