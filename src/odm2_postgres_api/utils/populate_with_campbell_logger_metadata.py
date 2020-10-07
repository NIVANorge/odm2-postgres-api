import logging
import uuid

import requests

from nivacloud_logging.log_utils import setup_logging

# These ID's match the logger station id's in the LOGGER_STATION table in nivabasen
logger_station_location_uuids = {
    "6": "71ab5842-d872-4dcd-86ef-53295196c71b",
    "1": "6d523464-f01e-43e2-8530-11c166da7152",
    "2": "a941bd83-5b0a-4a5e-b20f-0fa3328e0e56",
    "7": "1d648942-5aa8-468a-a5a5-0bc60afcc59d",
}

# The sampling_feature_name matches the name
sampling_features = {
    "sampling_features": [
        {
            "samplingfeatureuuid": logger_station_location_uuids["6"],
            "samplingfeaturetypecv": "Weather station",
            "samplingfeaturecode": "6",
            "elevation_m": 516,
            "samplingfeaturename": "Langtjern Værstasjon",
            "samplingfeaturedescription": "Langtjern Værstasjon, Matches station_name 'Værstasjon ved Langtjern'",
            "samplingfeaturegeotypecv": "Point",
            "featuregeometrywkt": "POINT (60.37155049 9.72747998)",
            "elevationdatumcv": "MSL",
        },
        {
            "samplingfeatureuuid": logger_station_location_uuids["1"],
            "samplingfeaturetypecv": "Water quality station",
            "samplingfeaturecode": "1",
            "elevation_m": 516,
            "samplingfeaturename": "Langtjern Bøye",
            "samplingfeaturedescription": "Langtjern Bøye, Matches station_name 'Bøye i Langtjern'",
            "samplingfeaturegeotypecv": "Point",
            "featuregeometrywkt": "POINT (60.36983000 9.72997000)",
            "elevationdatumcv": "MSL",
        },
        {
            "samplingfeatureuuid": logger_station_location_uuids["2"],
            "samplingfeaturetypecv": "Water quality station",
            "samplingfeaturecode": "2",
            "elevation_m": 516,
            "samplingfeaturename": "Langtjern utløp",
            "samplingfeaturedescription": "Langtjern utløp, Matches station_name 'Utløp av Langtjern'",
            "samplingfeaturegeotypecv": "Point",
            "featuregeometrywkt": "POINT (60.37267000 9.72664000)",
            "elevationdatumcv": "MSL",
        },
        {
            "samplingfeatureuuid": logger_station_location_uuids["7"],
            "samplingfeaturetypecv": "Water quality station",
            "samplingfeaturecode": "7",
            "elevation_m": 516,
            "samplingfeaturename": "Langtjern innløp LAE03",
            "samplingfeaturedescription": "Langtjern innløp LAE03, Matches station_name 'Innløpet LAE03 - Langtjern'",
            "samplingfeaturegeotypecv": "Point",
            "featuregeometrywkt": "POINT (60.37097966 9.73187018)",
            "elevationdatumcv": "MSL",
        },
    ],
    #     'spatial_references': {
    #     "srsname": "Langtjern Bøye",
    #     "srsdescription": "Langtjern Bøye"
    # }, 'sites': {
    #     "samplingfeatureid": 2,
    #     "sitetypecv": "Lake, Reservoir, Impoundment",
    #     "latitude": 60.36983000,
    #     "longitude": 9.72997000,
    #     "spatialreferenceid": 2
    # }
}

equipment = {
    "equipment_model": {
        "modelmanufacturerid": 1,
        "modelname": "A cool NIVA temperature sensor",
        "modeldescription": "A sensor model that measures temperature",
        "isinstrument": True,
    }
}

logger_results = [
    {
        "unitsid": 2,  # temperature
        "variableid": 2,  # temperature
        "begin_datetime": "2010-04-19T09:25:29",
        "result_uuid": "e4593455-3a08-45e1-9dbd-347fad6946dd",
        "equipment": {
            "equipmentcode": "NIVA-TEMP-1",
            "equipmentname": "niva tempreature sensor",
            "equipmenttypecv": "Sensor",
            "equipmentmodelid": 1,
            "equipmentserialnumber": "niva-01",
            "equipmentownerid": 1,
            "equipmentvendorid": 1,
            "equipmentpurchasedate": "2020-01-07T11:09:31.953",
        },
    }
]


def create_result(new_result):
    equipment_response = post_to_odm2_api("equipment", new_result["equipment"]) if "equipment" in new_result else None
    action_response = post_to_odm2_api(
        "actions",
        {
            "equipmentids": [equipment_response["equipmentid"]] if equipment_response else [],
            "affiliationid": 1,
            "isactionlead": True,
            "roledescription": "Deployed a specific instrument",
            "actiontypecv": "Instrument deployment",
            "methodcode": "000",
            "begindatetime": new_result["begin_datetime"],
            "begindatetimeutcoffset": 0,
        },
    )
    result_response = post_to_odm2_api(
        "results",
        {
            "samplingfeatureuuid": logger_station_location_uuids["1"],
            "actionid": action_response["actionid"],
            "resultuuid": new_result["result_uuid"],
            "resulttypecv": "Time series coverage",
            "variableid": new_result["variableid"],
            "unitsid": new_result["unitsid"],
            "processinglevelid": 1,
            "valuecount": 0,
            "statuscv": "Ongoing",
            "sampledmediumcv": "Liquid aqueous",
            "dataqualitycodes": [],
        },
    )


def post_to_odm2_api(endpoint, data):
    response = requests.post("http://localhost:8701/" + endpoint, json=data)
    response.raise_for_status()
    logging.info(f"Succesfully inserted into '{endpoint}': {response.json()}")
    return response.json()


def main():
    setup_logging(plaintext=True)
    data_sets = [sampling_features, equipment]
    for data_set in data_sets:
        for endpoint, data in data_set.items():
            if type(data) == list:
                for data_item in data:
                    post_to_odm2_api(endpoint, data_item)
            else:
                post_to_odm2_api(endpoint, data)
    for new_result in logger_results:
        create_result(new_result)


if __name__ == "__main__":
    main()
