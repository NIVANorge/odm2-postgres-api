from odm2_postgres_api.schemas.schemas import UnitsCreate

general_units = [
    {
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
    },
]


def units() -> UnitsCreate:
    return [UnitsCreate(**u) for u in general_units]
