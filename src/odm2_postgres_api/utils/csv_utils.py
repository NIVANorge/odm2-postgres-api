import csv
from typing import List, Dict

from io import StringIO

from odm2_postgres_api.schemas.schemas import BegroingObservation


def to_csv(observations: List[BegroingObservation]) -> str:
    with StringIO() as stream:
        names = ["project_name", "dato", "lok_sta", "rubin_kode", "Mengderef", "Mengde_Tekst", "Mengde_Tall", "Flagg"]

        writer = csv.DictWriter(stream, delimiter="\t", fieldnames=names, quoting=csv.QUOTE_ALL)

        writer.writeheader()
        for o in observations:
            row = {
                "project_name": o.project.directivedescription,
                "dato": o.timestamp.isoformat(),
                "lok_sta": o.station.samplingfeaturecode,
                "rubin_kode": o.taxon.taxonomicclassifiercommonname,
                # TODO: needs a mapping to access db method names?
                "Mengderef": o.method.methodname,
                "Mengde_Tekst": o.categorical_value,
                "Mengde_Tall": o.measurement_value,
                "Flagg": o.flag,
            }
            writer.writerow(row)

        return stream.getvalue()
