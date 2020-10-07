from typing import List

from odm2_postgres_api.schemas.schemas import VariablesCreate

general_variables = [
    {
        "variabletypecv": "Chemistry",
        "variablenamecv": "Salinity",
        "variabledefinition": "salinity",
        "variablecode": "001",
        "nodatavalue": -9999,
    },
    {
        "variabletypecv": "Climate",
        "variablenamecv": "Temperature",
        "variabledefinition": "temperature",
        "variablecode": "002",
        "nodatavalue": -9999,
    },
    {
        "variabletypecv": "Water quality",
        "variablenamecv": "Chlorophyll fluorescence",
        "variabledefinition": "fluorescence from  chlorophyll A",
        "variablecode": "003",
        "nodatavalue": -9999,
    },
    {
        "variabletypecv": "Water quality",
        "variablenamecv": "Chlorophyll fluorescence",
        "variabledefinition": "pah_fluorescence",
        "variablecode": "004",
        "nodatavalue": -9999,
    },
    {
        "variabletypecv": "Water quality",
        "variablenamecv": "Chlorophyll fluorescence",
        "variabledefinition": "fluorescence from cyanobacteria",
        "variablecode": "005",
        "nodatavalue": -9999,
    },
    {
        "variabletypecv": "Water quality",
        "variablenamecv": "Chlorophyll fluorescence",
        "variabledefinition": "cdom_fluorescence",
        "variablecode": "006",
        "nodatavalue": -9999,
    },
    {
        "variabletypecv": "Water quality",
        "variablenamecv": "Turbidity",
        "variabledefinition": "turbidity in water",
        "variablecode": "007",
        "nodatavalue": -9999,
    },
    {
        "variabletypecv": "Climate",
        "variablenamecv": "Barometric pressure",
        "variabledefinition": "atmospheric pressure",
        "variablecode": "008",
        "nodatavalue": -9999,
    },
    {
        "variabletypecv": "Chemistry",
        "variablenamecv": "Electrical conductivity",
        "variabledefinition": "electrical conductivity of substance",
        "variablecode": "009",
        "nodatavalue": -9999,
    },
    {
        "variabletypecv": "Biota",
        "variablenamecv": "Abundance",
        "variabledefinition": "This variable indicates the abundance of the taxon of the result",
        "variablecode": "010",
        "nodatavalue": -9999,
    },
]


def variables() -> List[VariablesCreate]:
    return [VariablesCreate(**v) for v in general_variables]
