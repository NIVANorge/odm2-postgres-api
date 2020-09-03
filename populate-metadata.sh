#!/bin/bash

set -x
set -e

curl -X PATCH "http://localhost:8701/controlled_vocabularies" -H "accept: application/json"
python src/odm2_postgres_api/utils/populate_with_general_metadata.py
python src/odm2_postgres_api/utils/populate_with_ferrybox_metadata.py
