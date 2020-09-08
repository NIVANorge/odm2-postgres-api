from typing import List, Tuple
from uuid import uuid4

from odm2_postgres_api.schemas.schemas import (
    SamplingFeaturesCreate,
    MethodsCreate,
)


def fish_rfid_methods(org_id: int):
    methods = [
        {
            "methodtypecv": "Observation",
            "methodcode": "fish_rfid:register_fish",
            "methodname": "Registering of tagged fishes",
            "methoddescription": "Fishes are catched, tagged with RFID chips and then released to be tracked by oregon RFID sensors",  # nopep8
            "methodlink": "https://github.com/NIVANorge/fish_rfid/",
            "organizationid": org_id,
        },
        {
            "methodtypecv": "Observation",
            "methodcode": "fish_rfid:observe_fish",
            "methodname": "Fish observed by rfid-sensor at station",
            "methoddescription": "Tagged fishes detected near a oregon RFID sensor",
            "methodlink": "https://github.com/NIVANorge/fish_rfid/",
            "organizationid": org_id,
        },
    ]

    return [MethodsCreate(**m) for m in methods]


def fish_rfid_sampling_features() -> List[SamplingFeaturesCreate]:
    stations = [
        {
            "samplingfeatureuuid": "49c799ba-f1df-11ea-9179-ffcbac9423dd",
            "samplingfeaturecode": "AAA",
            "samplingfeaturetypecv": "Site",
            # TODO: need to fill in actual values below..
            "samplingfeaturename": "fish rfid station",
            "samplingfeaturedescription": "Station detecting tagged nearby tagged fishes with Oregon RFID sensors",
            # TODO: just took a random WKT string, need to put in the actual one
            "featuregeometrywkt": "POINT (10.907013757789976 60.25819134332953)",
            "samplingfeaturegeotypecv": "Point",
        }
    ]

    return [SamplingFeaturesCreate(**s) for s in stations]
