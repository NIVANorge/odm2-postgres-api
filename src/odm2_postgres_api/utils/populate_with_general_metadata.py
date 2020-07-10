import logging

import requests

from nivacloud_logging.log_utils import setup_logging


people_and_organizations = {"organizations": {
    "organizationtypecv": "Research institute",
    "organizationcode": "niva-no",
    "organizationname": "NIVA Norsk institut for vannforskning",
    "organizationdescription": "The Norwegian institute for water research",
    "organizationlink": "www.niva.no"
}, "people": {
    "personfirstname": "Uknown begroing employee",
    "personlastname": "NIVA"
}, "affiliations": {
    "personid": 1,
    "affiliationstartdate": "2018-12-15",
    "primaryemail": "roald.storm@niva.no",
    "organizationid": 1,
    "isprimaryorganizationcontact": "false",
    "primaryphone": "0047 413 60 753"
}, "external_identifier_system": {
    "externalidentifiersystemname": 'niva-port',
    "identifiersystemorganizationid": 1,
    "externalidentifiersystemdescription": "",
    "externalidentifiersystemurl": ""
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

begroing_indices = [{
    "name": "PIT",
    "term": "PIT (Periphyton Index of Trophic status)",
    "definition": "An index to keep track of water quality as described here: https://link.springer.com/article/10.1007/s10750-011-0614-7",  # nopep8
    "category": "Biota",
    "controlled_vocabulary_table_name": "cv_variablename"
}, {
    "name": "AIP",
    "term": "AIP (Acidification Index Periphyton)",
    "definition": "An index to keep track of water quality as described here: https://www.sciencedirect.com/science/article/abs/pii/S1470160X09000491",  # nopep8
    "category": "Biota",
    "controlled_vocabulary_table_name": "cv_variablename"
}, {
    "name": "HBI",
    "term": "HBI (heterotrof begroingsindeks)",
    "definition": "An index to keep track of water quality that is based on heterotroph organisms",
    "category": "Biota",
    "controlled_vocabulary_table_name": "cv_variablename"
}, {
    "name": "HBI2",
    "term": "HBI2 (heterotrof begroingsindeks 2)",
    "definition": "An index to keep track of water quality that is based on heterotroph organisms version2",
    "category": "Biota",
    "controlled_vocabulary_table_name": "cv_variablename"
}, {
    "name": "PIT EQR",
    "term": "PIT EQR (Periphyton Index of Trophic status, Ecological Quality Ratio)",
    "definition": "An index to keep track of water quality as described here: https://link.springer.com/article/10.1007/s10750-011-0614-7",  # nopep8
    "category": "Biota",
    "controlled_vocabulary_table_name": "cv_variablename"
}, {
    "name": "AIP EQR",
    "term": "AIP EQR (Acidification Index Periphyton, Ecological Quality Ratio)",
    "definition": "An index to keep track of water quality as described here: https://www.sciencedirect.com/science/article/abs/pii/S1470160X09000491",  # nopep8
    "category": "Biota",
    "controlled_vocabulary_table_name": "cv_variablename"
}, {
    "name": "HBI EQR",
    "term": "HBI EQR (heterotrof begroingsindeks, Ecological Quality Ratio)",
    "definition": "An index to keep track of water quality that is based on heterotroph organisms",
    "category": "Biota",
    "controlled_vocabulary_table_name": "cv_variablename"
}, {
    "name": "HBI2 EQR",
    "term": "HBI2 EQR (heterotrof begroingsindeks 2, Ecological Quality Ratio)",
    "definition": "An index to keep track of water quality that is based on heterotroph organisms version2",
    "category": "Biota",
    "controlled_vocabulary_table_name": "cv_variablename"
}, {
    "name": "PIT nEQR",
    "term": "PIT nEQR (Periphyton Index of Trophic status, normalized Ecological Quality Ratio)",
    "definition": "An index to keep track of water quality as described here: https://link.springer.com/article/10.1007/s10750-011-0614-7",  # nopep8
    "category": "Biota",
    "controlled_vocabulary_table_name": "cv_variablename"
}, {
    "name": "AIP nEQR",
    "term": "AIP nEQR (Acidification Index Periphyton, normalized Ecological Quality Ratio)",
    "definition": "An index to keep track of water quality as described here: https://www.sciencedirect.com/science/article/abs/pii/S1470160X09000491",  # nopep8
    "category": "Biota",
    "controlled_vocabulary_table_name": "cv_variablename"
}, {
    "name": "HBI nEQR",
    "term": "HBI nEQR (heterotrof begroingsindeks, normalized Ecological Quality Ratio)",
    "definition": "An index to keep track of water quality that is based on heterotroph organisms",
    "category": "Biota",
    "controlled_vocabulary_table_name": "cv_variablename"
}, {
    "name": "HBI2 nEQR",
    "term": "HBI2 nEQR (heterotrof begroingsindeks 2, normalized Ecological Quality Ratio)",
    "definition": "An index to keep track of water quality that is based on heterotroph organisms version2",
    "category": "Biota",
    "controlled_vocabulary_table_name": "cv_variablename"
}]


controlled_vocabularies = {'controlled_vocabularies': [{
    "term": "trackSeriesCoverage",
    "name": "Track series coverage",
    "definition": "A series of ResultValues for a single Variable, measured with a moving platform or some sort of "
                  "variable location, using a single Method, with specific Units, having a specific ProcessingLevel, "
                  "and measured over time.",
    "category": "Coverage",
    "controlled_vocabulary_table_name": "cv_resulttype"
}, *begroing_indices]}

begroing_variables = [{
    "variabletypecv": "Water quality",
    "variablenamecv": index_dict["name"],
    "variabledefinition": "water quality index",
    "variablecode": f'begroing_{i+1}',  # The abundance variable down below is begroing_0
    "nodatavalue": -9999
} for i, index_dict in enumerate(begroing_indices)]

# Most units taken from http://vocabulary.odm2.org/units/
units_and_variables = {'units': [{
    "unitstypecv": "Salinity",
    "unitsabbreviation": "PSU",
    "unitsname": "practical salinity unit"
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
    "unitsname": "parts per billion"
}, {
    "unitstypecv": "Concentration or density mass per volume",
    "unitsabbreviation": "μg/m^3",
    "unitsname": "microgram per cubic meter"
}, {
    "unitstypecv": "Concentration or density mass per volume",
    "unitsabbreviation": "micro mol/l",
    "unitsname": "microgram mol per liter"
}, {
    "unitstypecv": "Turbidity",
    "unitsabbreviation": "FTU",
    "unitsname": "formazin turbidity unit"
}, {
    "unitstypecv": "Turbidity",
    "unitsabbreviation": "NTU",
    "unitsname": "nepphelometric turbidity unit"
}, {
    "unitstypecv": "Pressure or stress",
    "unitsabbreviation": "bar",
    "unitsname": "bar"
}, {
    "unitstypecv": "Electrical conductivity",
    "unitsabbreviation": "S/m",
    "unitsname": "siemens per meter"
}, {
    "unitstypecv": "Volumetric flow rate",
    "unitsabbreviation": "m3/s",
    "unitsname": "cubic meters per second"
}, {
    "unitstypecv": "Linear velocity",
    "unitsabbreviation": "m/s",
    "unitsname": "meter per second"
}, {
    "unitstypecv": "Dimensionless",
    "unitsabbreviation": "Microscopic semi quantitative abundance classifier",
    "unitsname": "Microscopic semi quantitative abundance classifier, x = observed, xx = common, xxx = frequent"
}, {
    "unitstypecv": "Dimensionless",
    "unitsabbreviation": "PrsAbs",
    "unitsname": "Presence or Absence",
    "unitslink": "http://qwwebservices.usgs.gov/service-domains.html"
}, {
    "unitstypecv": "Dimensionless",
    "unitsabbreviation": "Macroscopic species coverage 1-5 scale",
    "unitsname": "Macroscopic species coverage 1-5 scale, 1 = single / rare (<1% coverage), 2 = scattered / occasional (1 <5% coverage), 3 = regular / frequent (5 <25% coverage), 4 = frequent / abundant (25 <50% coverage) ), 5 = dominant / dominant (50-100% coverage)"  # nopep8
}, {
    "unitstypecv": "Dimensionless",
    "unitsabbreviation": "%",
    "unitsname": "Kiselalger Relative abundance",
    "unitslink": "http://qudt.org/vocab/unit#Percent; http://unitsofmeasure.org/ucum.html#para-29; http://his.cuahsi.org/mastercvreg/edit_cv11.aspx?tbl=Units&id=1125579048; http://www.unidata.ucar.edu/software/udunits/; http://qwwebservices.usgs.gov/service-domains.html"  # nopep8
}, {
    "unitstypecv": "Dimensionless",
    "unitsabbreviation": "%",
    "unitsname": "Percent"
}, {
    "unitstypecv": "Time",
    "unitsabbreviation": "s",
    "unitsname": "second"
}, {
    "unitstypecv": "Dimensionless",
    "unitsabbreviation": "-",
    "unitsname": "Dimensionless"
}], 'variables': [{
    "variabletypecv": "Chemistry",
    "variablenamecv": "Salinity",
    "variabledefinition": "salinity",
    "variablecode": "001",
    "nodatavalue": -9999
}, {
    "variabletypecv": "Climate",
    "variablenamecv": "Temperature",
    "variabledefinition": "temperature",
    "variablecode": "002",
    "nodatavalue": -9999
}, {
    "variabletypecv": "Water quality",
    "variablenamecv": "Chlorophyll fluorescence",
    "variabledefinition": "fluorescence from  chlorophyll A",
    "variablecode": "003",
    "nodatavalue": -9999
}, {
    "variabletypecv": "Water quality",
    "variablenamecv": "Chlorophyll fluorescence",
    "variabledefinition": "pah_fluorescence",
    "variablecode": "004",
    "nodatavalue": -9999
}, {
    "variabletypecv": "Water quality",
    "variablenamecv": "Chlorophyll fluorescence",
    "variabledefinition": "fluorescence from cyanobacteria",
    "variablecode": "005",
    "nodatavalue": -9999
}, {
    "variabletypecv": "Water quality",
    "variablenamecv": "Chlorophyll fluorescence",
    "variabledefinition": "cdom_fluorescence",
    "variablecode": "006",
    "nodatavalue": -9999
}, {
    "variabletypecv": "Water quality",
    "variablenamecv": "Turbidity",
    "variabledefinition": "turbidity in water",
    "variablecode": "007",
    "nodatavalue": -9999
}, {
    "variabletypecv": "Climate",
    "variablenamecv": "Barometric pressure",
    "variabledefinition": "atmospheric pressure",
    "variablecode": "008",
    "nodatavalue": -9999
}, {
    "variabletypecv": "Chemistry",
    "variablenamecv": "Electrical conductivity",
    "variabledefinition": "electrical conductivity of substance",
    "variablecode": "009",
    "nodatavalue": -9999
}, {
    "variabletypecv": "Biota",
    "variablenamecv": "Abundance",
    "variabledefinition": "This variable indicates the abundance of the taxon of the result",
    "variablecode": "begroing_0",
    "nodatavalue": -9999
}, *begroing_variables]}

methods = {'methods': [{
    "methodtypecv": "Instrument deployment",
    "methodcode": "000",
    "methodname": "Deploy an Instrument",
    "methoddescription": "A method for deploying instruments",
    "organizationid": 1  # organization 1 is NIVA
}, {
    "methodtypecv": "Derivation",
    "methodcode": "001",
    "methodname": "Derive an adjusted result from a raw result",
    "methoddescription": "A method for deriving a result from another result",
    "organizationid": 1  # organization 1 is NIVA
}, {
    "methodtypecv": "Specimen analysis",
    "methodcode": "002",
    "methodname": "Microscopic abundance",
    "methoddescription": "A method for observing the abundance of a species in a sample, this method is tied to the unit: 'Microscopic semi quantitative abundance classifier'. The observation is conducted by looking at the sample through a microscopic and classifying the abundance of an organism. Quite often several samples are collected and the abundance of the species is aggregated. The rule is that the most abundant occurrence in any of the collected samples is the observed value.",  # nopep8
    "organizationid": 1  # organization 1 is NIVA
}, {
    "methodtypecv": "Observation",
    "methodcode": "003",
    "methodname": "absence/presence",
    "methoddescription": "A person observes if a species is present in the observed area or not.",
    "organizationid": 1  # organization 1 is NIVA
}, {
    "methodtypecv": "Observation",
    "methodcode": "004",
    "methodname": "Macroscopic abundance",
    "methoddescription": "A semi quantitative observation is made assessing the abundance of a species using the unit:'Macroscopic species coverage 1-5 scale'",  # nopep8
    "organizationid": 1  # organization 1 is NIVA
}, {
    "methodtypecv": "Observation",
    "methodcode": "005",
    "methodname": "Macroscopic coverage",
    "methoddescription": "A quantitative observation is made assessing the abundance of a species in percentage of area covered. The area is usually a subset chosen to be representative of the larger area that is assesed.",  # nopep8
    "organizationid": 1  # organization 1 is NIVA
}, {
    "methodtypecv": "Observation",
    "methodcode": "006",
    "methodname": "Kiselalger relative abundance",
    "methoddescription": "Relative abundance is the percent composition of an organism of a particular kind relative to the total number of organisms in the area. This observation has percentage as it's unit and it needs a classifying taxon in the result to make sense.",  # nopep8
    "organizationid": 1  # organization 1 is NIVA
}, {
    "methodtypecv": "Derivation",
    "methodcode": "007",
    "methodname": "Begroing Index Calculation",
    "methoddescription": "Calculate an index value according to it's variable description, supported variable now are PIT, AIP, HBI, HBI2 and their normalized variants",  # nopep8
    "organizationid": 1  # organization 1 is NIVA
}]}


def post_to_odm2_api(endpoint, data):
    response = requests.post('http://localhost:8701/' + endpoint, json=data)
    try:
        response.raise_for_status()
    except Exception:
        logging.error(data)
        raise
    logging.info(f"Succesfully inserted into '{endpoint}': {response.json()}")
    return response.json()


def main():
    setup_logging(plaintext=True)
    data_sets = [people_and_organizations, processing_levels, controlled_vocabularies, units_and_variables, methods]
    for data_set in data_sets:
        for endpoint, data in data_set.items():
            if type(data) == list:
                for data_item in data:
                    post_to_odm2_api(endpoint, data_item)
            else:
                post_to_odm2_api(endpoint, data)


if __name__ == '__main__':
    main()
