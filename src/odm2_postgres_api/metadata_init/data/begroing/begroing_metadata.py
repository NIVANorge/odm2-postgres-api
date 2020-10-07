from typing import List

from odm2_postgres_api.schemas.schemas import VariablesCreate, MethodsCreate, ControlledVocabularyCreate, UnitsCreate

begroing_indices = [
    {
        "name": "PIT",
        "term": "PIT (Periphyton Index of Trophic status)",
        "definition":
        "An index to keep track of water quality as described here: https://link.springer.com/article/10.1007/s10750-011-0614-7",  # nopep8
        "category": "Biota",
        "controlled_vocabulary_table_name": "cv_variablename"
    },
    {
        "name": "AIP",
        "term": "AIP (Acidification Index Periphyton)",
        "definition":
        "An index to keep track of water quality as described here: https://www.sciencedirect.com/science/article/abs/pii/S1470160X09000491",  # nopep8
        "category": "Biota",
        "controlled_vocabulary_table_name": "cv_variablename"
    },
    {
        "name": "HBI",
        "term": "HBI (heterotrof begroingsindeks)",
        "definition": "An index to keep track of water quality that is based on heterotroph organisms",
        "category": "Biota",
        "controlled_vocabulary_table_name": "cv_variablename"
    },
    {
        "name": "HBI2",
        "term": "HBI2 (heterotrof begroingsindeks 2)",
        "definition": "An index to keep track of water quality that is based on heterotroph organisms version2",
        "category": "Biota",
        "controlled_vocabulary_table_name": "cv_variablename"
    },
    {
        "name": "PIT EQR",
        "term": "PIT EQR (Periphyton Index of Trophic status, Ecological Quality Ratio)",
        "definition":
        "An index to keep track of water quality as described here: https://link.springer.com/article/10.1007/s10750-011-0614-7",  # nopep8
        "category": "Biota",
        "controlled_vocabulary_table_name": "cv_variablename"
    },
    {
        "name": "AIP EQR",
        "term": "AIP EQR (Acidification Index Periphyton, Ecological Quality Ratio)",
        "definition":
        "An index to keep track of water quality as described here: https://www.sciencedirect.com/science/article/abs/pii/S1470160X09000491",  # nopep8
        "category": "Biota",
        "controlled_vocabulary_table_name": "cv_variablename"
    },
    {
        "name": "HBI EQR",
        "term": "HBI EQR (heterotrof begroingsindeks, Ecological Quality Ratio)",
        "definition": "An index to keep track of water quality that is based on heterotroph organisms",
        "category": "Biota",
        "controlled_vocabulary_table_name": "cv_variablename"
    },
    {
        "name": "HBI2 EQR",
        "term": "HBI2 EQR (heterotrof begroingsindeks 2, Ecological Quality Ratio)",
        "definition": "An index to keep track of water quality that is based on heterotroph organisms version2",
        "category": "Biota",
        "controlled_vocabulary_table_name": "cv_variablename"
    },
    {
        "name": "PIT nEQR",
        "term": "PIT nEQR (Periphyton Index of Trophic status, normalized Ecological Quality Ratio)",
        "definition":
        "An index to keep track of water quality as described here: https://link.springer.com/article/10.1007/s10750-011-0614-7",  # nopep8
        "category": "Biota",
        "controlled_vocabulary_table_name": "cv_variablename"
    },
    {
        "name": "AIP nEQR",
        "term": "AIP nEQR (Acidification Index Periphyton, normalized Ecological Quality Ratio)",
        "definition":
        "An index to keep track of water quality as described here: https://www.sciencedirect.com/science/article/abs/pii/S1470160X09000491",  # nopep8
        "category": "Biota",
        "controlled_vocabulary_table_name": "cv_variablename"
    },
    {
        "name": "HBI nEQR",
        "term": "HBI nEQR (heterotrof begroingsindeks, normalized Ecological Quality Ratio)",
        "definition": "An index to keep track of water quality that is based on heterotroph organisms",
        "category": "Biota",
        "controlled_vocabulary_table_name": "cv_variablename"
    },
    {
        "name": "HBI2 nEQR",
        "term": "HBI2 nEQR (heterotrof begroingsindeks 2, normalized Ecological Quality Ratio)",
        "definition": "An index to keep track of water quality that is based on heterotroph organisms version2",
        "category": "Biota",
        "controlled_vocabulary_table_name": "cv_variablename"
    }
]


def begroing_controlled_vocabularies() -> List[ControlledVocabularyCreate]:
    return [ControlledVocabularyCreate(**cv) for cv in begroing_indices]


begroing_units_list = [
    {
        "unitstypecv": "Dimensionless",
        "unitsabbreviation": "Microscopic semi quantitative abundance classifier",
        "unitsname": "Microscopic semi quantitative abundance classifier, x = observed, xx = common, xxx = frequent"
    },
    {
        "unitstypecv":
        "Dimensionless",
        "unitsabbreviation":
        "Macroscopic species coverage 1-5 scale",
        "unitsname":
        "Macroscopic species coverage 1-5 scale, 1 = single / rare (<1% coverage), 2 = scattered / occasional (1 <5% coverage), 3 = regular / frequent (5 <25% coverage), 4 = frequent / abundant (25 <50% coverage) ), 5 = dominant / dominant (50-100% coverage)"  # nopep8
    }
]


def begroing_units() -> List[UnitsCreate]:
    return [UnitsCreate(**u) for u in begroing_units_list]


def begroing_variables() -> List[VariablesCreate]:
    begroing_variables_list = [
        {
            "variabletypecv": "Water quality",
            "variablenamecv": index_dict["name"],
            "variabledefinition": "water quality index",
            "variablecode": f'begroing_{i}',  # The abundance variable down below is begroing_0
            "nodatavalue": -9999
        } for i, index_dict in enumerate(begroing_indices)
    ]

    return [VariablesCreate(**v) for v in begroing_variables_list]


def begroing_methods(org_id: int) -> List[MethodsCreate]:
    annotations = [{
        "annotationtypecv": "Method annotation",
        "annotationtext": "Method is part of 'begroing' workflows",
    }]
    methods = [
        {
            "methodtypecv": "Specimen analysis",
            "methodcode": "begroing_1",
            "methodname": "Microscopic abundance",
            "methoddescription":
            "A method for observing the abundance of a species in a sample, this method is tied to the unit: 'Microscopic semi quantitative abundance classifier'. The observation is conducted by looking at the sample through a microscopic and classifying the abundance of an organism. Quite often several samples are collected and the abundance of the species is aggregated. The rule is that the most abundant occurrence in any of the collected samples is the observed value.",  # nopep8
            "organizationid": org_id,
            "annotations": annotations
        },
        {
            "methodtypecv": "Observation",
            "methodcode": "begroing_2",
            "methodname": "absence/presence",
            "methoddescription": "A person observes if a species is present in the observed area or not.",
            "organizationid": org_id,
            "annotations": annotations
        },
        {
            "methodtypecv": "Observation",
            "methodcode": "begroing_3",
            "methodname": "Macroscopic abundance",
            "methoddescription":
            "A semi quantitative observation is made assessing the abundance of a species using the unit:'Macroscopic species coverage 1-5 scale'",  # nopep8
            "organizationid": org_id,
            "annotations": annotations
        },
        {
            "methodtypecv": "Observation",
            "methodcode": "begroing_4",
            "methodname": "Macroscopic coverage",
            "methoddescription":
            "A quantitative observation is made assessing the abundance of a species in percentage of area covered. The area is usually a subset chosen to be representative of the larger area that is assesed.",  # nopep8
            "organizationid": org_id,
            "annotations": annotations
        },
        {
            "methodtypecv": "Observation",
            "methodcode": "begroing_5",
            "methodname": "Kiselalger relative abundance",
            "methoddescription":
            "Relative abundance is the percent composition of an organism of a particular kind relative to the total number of organisms in the area. This observation has percentage as it's unit and it needs a classifying taxon in the result to make sense.",  # nopep8
            "organizationid": org_id,
            "annotations": annotations
        },
        {
            "methodtypecv": "Derivation",
            "methodcode": "begroing_6",
            "methodname": "Begroing Index Calculation",
            "methoddescription":
            "Calculate an index value according to it's variable description, supported variable now are PIT, AIP, HBI, HBI2 and their normalized variants",  # nopep8
            "organizationid": org_id,
            "annotations": annotations
        }
    ]

    return [MethodsCreate(**m) for m in methods]
