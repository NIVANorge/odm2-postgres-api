import csv
from typing import List, Dict

from io import StringIO

from odm2_postgres_api.schemas.schemas import BegroingObservation


def to_csv(observations: List[BegroingObservation], columns):
    with StringIO() as stream:
        writer = csv.writer(stream, delimiter=";")

        header = ["project_name", "timestamp", "station_code", "rubin_kode"]
        for o in observations:
            row = [
                o.project.directivedescription,
                o.timestamp,
                o.station.samplingfeaturename,
                o.taxon.taxonomicclassifiername,
            ]

        return stream
