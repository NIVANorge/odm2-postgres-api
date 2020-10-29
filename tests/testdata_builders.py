import uuid
from typing import Dict, Optional

from odm2_postgres_api.schemas.schemas import (
    Directive,
    SamplingFeatures,
    TaxonomicClassifierCreate,
    Methods,
    TaxonomicClassifier,
)


def default_station(overrides: Optional[Dict] = None) -> SamplingFeatures:
    o = {} if overrides is None else overrides
    defaults = {
        "samplingfeatureid": 55,
        "samplingfeatureuuid": uuid.uuid4(),
        "samplingfeaturecode": "Site",
        "samplingfeaturename": "Station xzy",
        "samplingfeaturedescription": "River station lorem ipsum",
        "samplingfeaturegeotypecv": "Point",
        "samplingfeaturetypecv": "Site",
        "featuregeometrywkt": "POINT (60.37155049 9.72747998)",
    }
    return SamplingFeatures(**{**defaults, **o})


def default_project(overrides: Optional[Dict] = None) -> Directive:
    o = {} if overrides is None else overrides
    defaults = {"directiveid": 1, "directivetypecv": "Project", "directivedescription": "awesome project"}

    return Directive(**{**defaults, **o})


def generate_taxon(overrides: Optional[Dict] = None) -> TaxonomicClassifier:
    o = {} if overrides is None else overrides
    t = {
        "taxonomicclassifierid": 55,
        "taxonomicclassifiercommonname": str(uuid.uuid4()),
        "taxonomicclassifierdescription": str(uuid.uuid4()),
        "taxonomicclassifiertypecv": "Biology",
        "taxonomicclassifiername": str(uuid.uuid4()),
    }

    return TaxonomicClassifier(**{**t, **o})


def generate_taxon_create(overrides: Optional[Dict] = None) -> TaxonomicClassifierCreate:
    o = {} if overrides is None else overrides
    t = {
        "taxonomicclassifiercommonname": str(uuid.uuid4()),
        "taxonomicclassifierdescription": str(uuid.uuid4()),
        "taxonomicclassifiertypecv": "Biology",
        "taxonomicclassifiername": str(uuid.uuid4()),
    }

    return TaxonomicClassifierCreate(**{**t, **o})


def default_method(overrides: Optional[Dict] = None) -> Methods:
    o = {} if overrides is None else overrides
    macroscopic_coverage = {
        "methodcode": "begroing_4",
        "methoddescription": "A quantitative observation is made assessing the abundance of a species in...",
        "methodid": 6,
        "methodlink": None,
        "methodname": "Macroscopic coverage",
        "methodtypecv": "Observation",
    }

    return Methods(**{**macroscopic_coverage, **o})
