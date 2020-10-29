import uuid
from typing import Dict

from odm2_postgres_api.schemas.schemas import (
    Directive,
    SamplingFeatures,
    TaxonomicClassifierCreate,
    Methods,
    TaxonomicClassifier,
)


def default_sampling_feature(overrides: Dict = {}) -> SamplingFeatures:
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
    return SamplingFeatures(**{**defaults, **overrides})


def default_project(overrides: Dict = {}) -> Directive:
    defaults = {"directiveid": 1, "directivetypecv": "Project", "directivedescription": "awesome project"}

    return Directive(**{**defaults, **overrides})


def generate_taxon(overrides: Dict = {}) -> TaxonomicClassifier:
    t = {
        "taxonomicclassifierid": 55,
        "taxonomicclassifiercommonname": str(uuid.uuid4()),
        "taxonomicclassifierdescription": str(uuid.uuid4()),
        "taxonomicclassifiertypecv": "Biology",
        "taxonomicclassifiername": str(uuid.uuid4()),
    }

    return TaxonomicClassifier(**{**t, **overrides})


def generate_taxon_create(overrides: Dict = {}) -> TaxonomicClassifierCreate:
    t = {
        "taxonomicclassifiercommonname": str(uuid.uuid4()),
        "taxonomicclassifierdescription": str(uuid.uuid4()),
        "taxonomicclassifiertypecv": "Biology",
        "taxonomicclassifiername": str(uuid.uuid4()),
    }

    return TaxonomicClassifierCreate(**{**t, **overrides})


def default_method(overrides: Dict = {}) -> Methods:
    macroscopic_coverage = {
        "methodcode": "begroing_4",
        "methoddescription": "A quantitative observation is made assessing the abundance of a species in...",
        "methodid": 6,
        "methodlink": None,
        "methodname": "Macroscopic coverage",
        "methodtypecv": "Observation",
    }

    return Methods(**{**macroscopic_coverage, **overrides})
