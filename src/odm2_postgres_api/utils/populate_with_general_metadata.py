import logging

import requests

from nivacloud_logging.log_utils import setup_logging


people_and_organizations = {"people": [{
    "personfirstname": "Roald",
    "personlastname": "Storm"
}, {
    "personfirstname": "Uknown begroing employee",
    "personlastname": "NIVA"
}], "organizations": {
    "organizationtypecv": "Research institute",
    "organizationcode": "niva-no",
    "organizationname": "NIVA Norsk institut for vannforskning",
    "organizationdescription": "The Norwegian institute for water research",
    "organizationlink": "www.niva.no"
}, "affiliations": [{
    "personid": 1,
    "affiliationstartdate": "2018-12-15",
    "primaryemail": "roald.storm@niva.no",
    "organizationid": 1,
    "isprimaryorganizationcontact": "false",
    "primaryphone": "0047 413 60 753"
}, {
    # Todo: Add in contact information for someone relevant instead of me
    "personid": 2,
    "affiliationstartdate": "1958-01-01",
    "primaryemail": "roald.storm@niva.no",
    "organizationid": 1,
    "isprimaryorganizationcontact": "false",
    "primaryphone": "0047 413 60 753"
}]}


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
    "variablecode": "010",
    "nodatavalue": -9999
}]}

other_stuff = {'controlled_vocabularies': {
    "term": "trackSeriesCoverage",
    "name": "Track series coverage",
    "definition": "A series of ResultValues for a single Variable, measured with a moving platform or some sort of "
                  "variable location, using a single Method, with specific Units, having a specific ProcessingLevel, "
                  "and measured over time.",
    "category": "Coverage",
    "controlled_vocabulary_table_name": "cv_resulttype"
}, 'methods': [{
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
    "methodname": "Microspcopic species abundance observation",
    "methoddescription": "A method for observing the abundance of a species in a sample, this method is tied the unit: 'Microscopic semi quantitative abundance classifier'. The observation is conducted by looking at the sample through a microspic and classifying the abundance of an organism.",  # nopep8
    "organizationid": 1  # organization 1 is NIVA
}, {
    "methodtypecv": "Observation",
    "methodcode": "003",
    "methodname": "Species absence/presence observation",
    "methoddescription": "A person observes if a species is present in the observed area or not.",
    "organizationid": 1  # organization 1 is NIVA
}, {
    "methodtypecv": "Observation",
    "methodcode": "004",
    "methodname": "Macroscopic species abundance observation",
    "methoddescription": "A semi quantitative observation is made assessing the abundance of a species using the unit:'Macroscopic species coverage 1-5 scale'",  # nopep8
    "organizationid": 1  # organization 1 is NIVA
}, {
    "methodtypecv": "Observation",
    "methodcode": "005",
    "methodname": "Macroscopic species coverage observation",
    "methoddescription": "A quantitative observation is made assessing the abundance of a species in percentage of area covered.",  # nopep8
    "organizationid": 1  # organization 1 is NIVA
}, {
    "methodtypecv": "Observation",
    "methodcode": "006",
    "methodname": "Kiselalger relative species abundance observation",
    "methoddescription": "Relative abundance is the percent composition of an organism of a particular kind relative to the total number of organisms in the area. This observation has percentage as it's unit and it needs a classifying taxon in the result to make sense.",  # nopep8
    "organizationid": 1  # organization 1 is NIVA
}]}


def post_to_odm2_api(endpoint, data):
    response = requests.post('http://localhost:8701/' + endpoint, json=data)
    response.raise_for_status()
    logging.info(f"Succesfully inserted into '{endpoint}': {response.json()}")
    return response.json()


def main():
    setup_logging(plaintext=True)
    data_sets = [people_and_organizations, processing_levels, units_and_variables, other_stuff]
    for data_set in data_sets:
        for endpoint, data in data_set.items():
            if type(data) == list:
                for data_item in data:
                    post_to_odm2_api(endpoint, data_item)
            else:
                post_to_odm2_api(endpoint, data)


if __name__ == '__main__':
    main()
