import csv
from datetime import datetime, timedelta
from io import StringIO
from typing import List
from unittest.mock import patch
from uuid import uuid4

import pytest

from odm2_postgres_api.routes.begroing_routes import (
    post_indices,
    post_begroing_result,
    get_begroing_results,
    get_begroing_results_csv,
)
from odm2_postgres_api.routes.shared_routes import (
    post_directive,
    post_sampling_features,
    post_taxonomic_classifiers,
)
from odm2_postgres_api.schemas.schemas import (
    DirectivesCreate,
    SamplingFeaturesCreate,
    BegroingIndicesCreate,
    BegroingResultCreate,
    Methods,
    TaxonomicClassifierCreate,
    BegroingObservation,
    Directive,
)
from integration_test_fixtures import db_conn, wait_for_db
from odm2_postgres_api.utils.csv_utils import to_csv
from testdata_builders import (
    default_project,
    default_sampling_feature,
    generate_taxon_create,
    default_method,
    generate_taxon,
)

USER_HEADER = "eyJpZCI6IDIyMSwgInVpZCI6ICIxZWQyMDBkMy1mMDlhLTQxNjQtOTExMC1hMWYyNGY4OTliYjMiLCAiZGlzcGxheU5hbWUiOiAiXHUwMGM1Z2UgT2xzZW4iLCAiZW1haWwiOiAiZGV2dXNlckBzb21lZW1haWwuY29tIiwgInByb3ZpZGVyIjogIkRldkxvZ2luIiwgImNyZWF0ZVRpbWUiOiAiMjAyMC0wNC0yMFQxMTo0NToyMS4yNDFaIiwgInVwZGF0ZVRpbWUiOiAiMjAyMC0wNC0yMFQxMTo0NToyMS4yNDFaIiwgInJvbGVzIjogWyJhcHBzOmFkbWluIiwgIm5pdmEiXX0="


@pytest.mark.asyncio
@pytest.mark.docker
async def test_post_new_indices(db_conn):
    project = {"directivedescription": "BEGLOS", "directivetypecv": "Project"}

    project_response = await post_directive(DirectivesCreate(**project), db_conn)
    assert project_response.directiveid > 0

    sf = {
        "samplingfeatureuuid": "75ef7a47-78b5-4596-9935-62072797357a",
        "samplingfeaturetypecv": "Site",
        "samplingfeaturecode": "BEGROING_STATION",
    }

    created_sampling_feature = await post_sampling_features(SamplingFeaturesCreate(**sf), db_conn)
    assert created_sampling_feature.samplingfeatureid > 0

    index_data = {
        "project_ids": [project_response.directiveid],
        "station_uuid": created_sampling_feature.samplingfeatureuuid,
        "date": "2020-09-01T00:00:00.000Z",
        "indices": [{"indexType": "PIT EQR", "indexValue": "11"}],
    }
    response = await post_indices(BegroingIndicesCreate(**index_data), db_conn, USER_HEADER)
    assert response
    # TODO: the endpoint does not really respond with anything worth asserting on


@patch("odm2_postgres_api.routes.begroing_routes.store_begroing_results", autospec=True)
@pytest.mark.asyncio
@pytest.mark.docker
async def test_post_new_begroing_observations(store_begroing_results, db_conn):
    taxa_create = [generate_taxon_create() for i in range(0, 3)]
    taxa = [await post_taxonomic_classifiers(t, db_conn) for t in taxa_create]

    macroscopic_coverage = Methods(
        **{
            "methodcode": "begroing_4",
            "methoddescription": "A quantitative observation is made assessing the abundance of a species in...",
            "methodid": 6,
            "methodlink": None,
            "methodname": "Macroscopic coverage",
            "methodtypecv": "Observation",
        }
    )
    microscoping_abundance = Methods(
        **{
            "methodid": 3,
            # TODO: this does not match methods in nivabasen
            "methodname": "Microscopic abundance",
            "methoddescription": "A method for observing the abundance of a species in a sample",
            "methodtypecv": "Observation",
            "methodcode": "begroing_1",
        }
    )
    methods = [
        microscoping_abundance,
        macroscopic_coverage,
    ]

    observations = [
        ["x", ""],
        ["xxx", ""],
        ["", "<1"],
    ]

    project_create = DirectivesCreate(
        directivedescription="Test project used in test_post_new_begroing_observations",
        directivetypecv="Project",
    )
    project = await post_directive(project_create, db_conn)

    station_create = SamplingFeaturesCreate(
        samplingfeatureuuid=uuid4(),
        samplingfeaturecode="TEST_STATION",  # 'DGL SJU' in begroing
        samplingfeaturetypecv="Site",
        samplingfeaturename="TEST_STATION, Test",
        featuregeometrywkt="POINT (10.791888159215542 59.90455564858938)",
    )
    station = await post_sampling_features(station_create, db_conn)

    date = datetime.now()
    begroing_result = BegroingResultCreate(
        projects=[project],
        date=date,
        station=station,
        taxons=taxa,
        methods=methods,
        observations=observations,
    )

    await post_begroing_result(begroing_result=begroing_result, connection=db_conn, niva_user=USER_HEADER)

    assert not store_begroing_results.called

    result = await get_begroing_results(
        sampling_feature_uuid=station.samplingfeatureuuid,
        project_id=project.directiveid,
        start_time=date,
        end_time=date,
        connection=db_conn,
        niva_user=USER_HEADER,
    )

    assert len(result) == len(observations)

    for obs in result:
        assert obs.project == project
        assert obs.station == station
        assert obs.timestamp == date

    for i, expected in enumerate(observations):
        expected_taxon = taxa[i]
        expected_value = expected[0] or expected[1]

        actual = next(o for o in result if o.taxon.taxonomicclassifierid == taxa[i].taxonomicclassifierid)
        assert expected_taxon == actual.taxon
        if expected_value == "<1":
            assert actual.measurement_value == 1.0
            assert actual.flag == "Less than"
            assert actual.method.methodcode == macroscopic_coverage.methodcode
        else:
            assert expected_value == actual.categorical_value
            assert actual.method.methodcode == microscoping_abundance.methodcode

    result_csv = await get_begroing_results_csv(
        sampling_feature_uuid=station.samplingfeatureuuid,
        project_id=project.directiveid,
        start_time=date,
        end_time=date,
        connection=db_conn,
        niva_user=USER_HEADER,
    )

    # Comparing csv result to json result, should be the same
    reader = csv.DictReader(StringIO(result_csv), delimiter="\t", quoting=csv.QUOTE_ALL)
    csv_rows = [r for r in reader]
    for i, row in enumerate(csv_rows):
        assert result[i].station.samplingfeaturecode == row["lok_sta"]
        assert result[i].project.directivedescription == row["project_name"]
        assert result[i].timestamp.isoformat() == row["dato"]
        # csv has no way to distinguish empty string from null, so none values are "" in the csv
        if result[i].measurement_value is not None:
            assert float(row["Mengde_Tall"]) == result[i].measurement_value
        assert row["Mengde_Tekst"] in [result[i].categorical_value, ""]
        assert row["Flagg"] in [result[i].flag, ""]

    # should not get any observations when querying for a different time
    different_time = await get_begroing_results(
        sampling_feature_uuid=station.samplingfeatureuuid,
        project_id=project.directiveid,
        start_time=date - timedelta(hours=2),
        end_time=date - timedelta(hours=1),
        connection=db_conn,
        niva_user=USER_HEADER,
    )

    assert len(different_time) == 0
