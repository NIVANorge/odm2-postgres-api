import os
import csv
import datetime as dt
from io import BytesIO, StringIO

environment = os.environ.get('NIVA_ENVIRONMENT')
if environment in ['dev', 'master']:
    from gcloud_common_utils.blob_helper import upload_blob
elif environment == 'localdev':
    from gcloud_common_utils.blob_helper_local import upload_blob
else:
    raise ValueError(f"NIVA_ENVIRONMENT set to {environment} please use: 'localdev', 'dev' or 'master'")


def generate_csv_from_form(form):
    csv_rows = []
    for index, species in enumerate(form['taxons'][:-1]):
        methods = [i for i, e in enumerate(form['observations'][index]) if e]
        if len(methods) != 1:
            raise ValueError('Must have one and only one method per species')
        method = form['methods'][methods[0]]['methodname']
        value = form['observations'][index][methods[0]]

        data_row = {'Prosjektnavn': None,
                    'lok_sta': form['station']['samplingfeaturename'].split(',')[0],
                    'dato': form['date'],
                    'rubin_kode': species['taxonomicclassifiercommonname'].split(',')[0],
                    'mengderef': '% dekning'}
        if method == 'Microscopic abundance':
            data_row['Mengde_tall'] = ''
            data_row['Flagg'] = ''
            data_row['Mengde_tekst'] = value
        elif method == 'Macroscopic coverage':
            # if form['observations'][index][method_index] == '<1'
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
        upload_blob(bucket_name, file_name, csv_file)
