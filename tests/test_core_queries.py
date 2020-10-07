import json

import pytest
from asyncpg import ForeignKeyViolationError
from pydantic import ValidationError

from integration_test_fixtures import wait_for_db, db_conn
from odm2_postgres_api.queries.core_queries import insert_taxonomic_classifier
from odm2_postgres_api.schemas import schemas


BEGROING_TAXON_ROWS = [
    {'Artsliste_Begroing_Sys_Id': 1, 'rubin_kode': 'AC AU;HE', 'latinsk_navn': 'Achnanthes austriaca var. helvetica', 'autor': 'Hust.', 'autor_ref': None, 'synonym': 'ACHN HEL', 'takson_type': 'VARIET', 'eier_takson': 'BACILLAR', 'ph_opt': 5.4, 'ph_ref': 'Stevenson', 'Kommentar': None, 'Reg_Dato': None, 'Reg_Av': None, 'Mod_Dato': '2016-02-04 15:56:28', 'Mod_Av': None, 'FF-verdi': None, 'rubin_nr': None, 'merke': None, 'Field1': 'S', 'egenskaper': '5,4', 'dato': '2002-12-09 00:00:00', 'RegHvor': 'Stevenson', 'Typesamling': None, 'Foto/tegning': None, 'ArtslisteId': 1},
    {'Artsliste_Begroing_Sys_Id': 2, 'rubin_kode': 'AC CL;RO', 'latinsk_navn': 'Achnanthes clevi var. rostrata', 'autor': 'Hust.', 'autor_ref': None, 'synonym': None, 'takson_type': 'VARIET', 'eier_takson': 'BACILLAR', 'ph_opt': None, 'ph_ref': None, 'Kommentar': None, 'Reg_Dato': None, 'Reg_Av': None, 'Mod_Dato': '2016-02-04 15:56:28', 'Mod_Av': None, 'FF-verdi': None, 'rubin_nr': None, 'merke': None, 'Field1': 'V', 'egenskaper': None, 'dato': None, 'RegHvor': None, 'Typesamling': None, 'Foto/tegning': None, 'ArtslisteId': 2},
    {'Artsliste_Begroing_Sys_Id': 3, 'rubin_kode': 'AC LA;EL', 'latinsk_navn': 'Achnanthes lanceolata var. eliptica', 'autor': 'Cleve', 'autor_ref': None, 'synonym': None, 'takson_type': 'VARIET', 'eier_takson': 'BACILLAR', 'ph_opt': None, 'ph_ref': None, 'Kommentar': None, 'Reg_Dato': None, 'Reg_Av': None, 'Mod_Dato': '2016-02-04 15:56:28', 'Mod_Av': None, 'FF-verdi': None, 'rubin_nr': None, 'merke': None, 'Field1': 'V', 'egenskaper': None, 'dato': None, 'RegHvor': None, 'Typesamling': None, 'Foto/tegning': None, 'ArtslisteId': 3},
    {'Artsliste_Begroing_Sys_Id': 4, 'rubin_kode': 'AC LI;PU', 'latinsk_navn': 'Achnanthes linearis var. pusilla', 'autor': 'Grun.', 'autor_ref': None, 'synonym': 'ACHN LIN', 'takson_type': 'VARIET', 'eier_takson': 'BACILLAR', 'ph_opt': None, 'ph_ref': None, 'Kommentar': None, 'Reg_Dato': None, 'Reg_Av': None, 'Mod_Dato': '2016-02-04 15:56:28', 'Mod_Av': None, 'FF-verdi': None, 'rubin_nr': None, 'merke': None, 'Field1': 'A', 'egenskaper': None, 'dato': None, 'RegHvor': None, 'Typesamling': None, 'Foto/tegning': None, 'ArtslisteId': 4},
    {'Artsliste_Begroing_Sys_Id': 5, 'rubin_kode': 'AC MI;CR', 'latinsk_navn': 'Achnanthes minutissima var. cryptocephala', 'autor': 'Grun.', 'autor_ref': None, 'synonym': 'ACHN MIN', 'takson_type': 'VARIET', 'eier_takson': 'BACILLAR', 'ph_opt': None, 'ph_ref': None, 'Kommentar': None, 'Reg_Dato': None, 'Reg_Av': None, 'Mod_Dato': '2016-02-04 15:56:28', 'Mod_Av': None, 'FF-verdi': None, 'rubin_nr': None, 'merke': None, 'Field1': 'A', 'egenskaper': None, 'dato': None, 'RegHvor': None, 'Typesamling': None, 'Foto/tegning': None, 'ArtslisteId': 5}
]


def format_taxon_data(row, related_taxonomic_classifiers=None):
    if related_taxonomic_classifiers is None:
        related_taxonomic_classifiers = []
    annotation = {
        "annotationtypecv": "Taxonomic classifier annotation",
        "annotationcode": "begroing-t_Artsliste_Begroing",
        "annotationtext": f'Artsliste_Begroing_Sys_Id={row["Artsliste_Begroing_Sys_Id"]}',
        "annotationjson": json.dumps(row, default=str),
    }
    data = {
        "taxonomicclassifiertypecv": "Biology",
        "taxonomicclassifiername": row["latinsk_navn"],
        "taxonomicclassifiercommonname": row["rubin_kode"],
        "taxonomicclassifierdescription": "Taken from begroing access database, "
                                          "check annotation for original row",
        "relatedtaxonomicclassifiers": related_taxonomic_classifiers,
        "annotations": [annotation],
    }
    return data


@pytest.mark.docker
@pytest.mark.asyncio
async def test_create_taxons_with_annotations(db_conn):
    for row in BEGROING_TAXON_ROWS[:-1]:
        data = format_taxon_data(row)
        taxon_result = await insert_taxonomic_classifier(db_conn, schemas.TaxonomicClassifierCreate(**data))
        assert type(taxon_result) == schemas.TaxonomicClassifier

    with pytest.raises(ForeignKeyViolationError):  # 0 can never exist since 'serial' datatype starts at 1
        data = format_taxon_data(BEGROING_TAXON_ROWS[-1], [(0, 'Is previous version of')])
        await insert_taxonomic_classifier(db_conn, schemas.TaxonomicClassifierCreate(**data))

    # Testing that an insert with a valid related action succeeds
    data = format_taxon_data(BEGROING_TAXON_ROWS[-1], [(taxon_result.taxonomicclassifierid, 'Is previous version of')])
    await insert_taxonomic_classifier(db_conn, schemas.TaxonomicClassifierCreate(**data))


def test_create_taxons_with_wrong_annotation():
    data = format_taxon_data(BEGROING_TAXON_ROWS[0])
    # Currently it is enforced that annotations for taxons always need "Taxonomic classifier annotation" as type
    with pytest.raises(ValidationError):
        data["annotations"][0]["annotationtypecv"] = "A wrong CV term"
        schemas.TaxonomicClassifierCreate(**data)
