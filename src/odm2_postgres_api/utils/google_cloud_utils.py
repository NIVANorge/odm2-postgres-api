import os
import csv
import datetime as dt
from io import BytesIO, StringIO

from odm2_postgres_api.schemas import schemas


class UploadBlobWrapper:
    def __init__(self):
        self._uploader = None

    def __call__(self, *args, **kwargs):
        if self._uploader is None:
            self.set_uploader()
        self._uploader(*args, **kwargs)

    def set_uploader(self):
        environment = os.environ.get('NIVA_ENVIRONMENT')
        if environment in ['dev', 'master']:
            from gcloud_common_utils.blob_helper import upload_blob
        elif environment == 'localdev':
            from gcloud_common_utils.blob_helper_local import upload_blob
        else:
            raise ValueError(f"NIVA_ENVIRONMENT set to {environment} please use: 'localdev', 'dev' or 'master'")
        self._uploader = upload_blob


upload_blob_wrapper = UploadBlobWrapper()


def generate_csv_from_form(begroing_result: schemas.BegroingResultCreate):
    date_string = (begroing_result.date + dt.timedelta(hours=6)).date().strftime('%d-%m-%Y %H:%M:%S')  # round date
    # date_string = begroing_result.date.strftime('%d-%m-%Y %H:%M:%S')
    csv_rows = []
    for index, species in enumerate(begroing_result.taxons):
        used_method_indices = [i for i, e in enumerate(begroing_result.observations[index]) if e]
        if len(used_method_indices) != 1:
            raise ValueError('Must have one and only one method per species')
        used_method_index = used_method_indices[0]
        method = begroing_result.methods[used_method_index].methodname
        value = begroing_result.observations[index][used_method_index]

        data_row = {'Prosjektnavn': '&&'.join([e.directivedescription for e in begroing_result.projects]),
                    'lok_sta': begroing_result.station['samplingfeaturename'].split(',')[0],
                    'dato': date_string,
                    'rubin_kode': species['taxonomicclassifiercommonname'].split(',')[0],
                    'mengderef': '% dekning'}

        if method == 'Microscopic abundance':
            data_row['Mengde_tall'] = ''
            data_row['Flagg'] = ''
            data_row['Mengde_tekst'] = value
        elif method == 'Macroscopic coverage':
            # if begroing_result.observations[index][method_index] == '<1'
            data_row['Mengde_tall'] = 1 if value == '<1' else value
            data_row['Flagg'] = '<' if value == '<1' else ''
            data_row['Mengde_tekst'] = ''
        else:
            raise ValueError("Currently only methods 'Microscopic abundance' and 'Macroscopic coverage' are allowed")
        csv_rows.append(data_row)
    return csv_rows


def put_csv_to_bucket(csv_data):
    bucket_name = os.environ['DATA_UPLOAD_BUCKET']
    file_name = f'begroing/{dt.datetime.now().isoformat()}_data.csv'

    with StringIO() as csv_file:
        fieldnames = list(csv_data[0].keys())
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

        writer.writeheader()
        for row in csv_data:
            writer.writerow(row)

        content = csv_file.getvalue().encode('utf-8')

    with BytesIO(initial_bytes=content) as csv_file:
        upload_blob_wrapper(bucket_name, file_name, csv_file)
