import os
from datetime import datetime
from unittest.mock import patch
from uuid import uuid4

import pytest

from odm2_postgres_api.routes.begroing_routes import post_indices, post_begroing_result
from odm2_postgres_api.routes.shared_routes import post_directive, post_sampling_features, post_taxonomic_classifiers
from odm2_postgres_api.schemas.schemas import DirectivesCreate, SamplingFeaturesCreate, BegroingIndicesCreate, \
    BegroingResultCreate, Methods, TaxonomicClassifierCreate
from integration_test_fixtures import db_conn, wait_for_db

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
        "samplingfeaturecode": "BEGROING_STATION"
    }

    created_sampling_feature = await post_sampling_features(SamplingFeaturesCreate(**sf), db_conn)
    assert created_sampling_feature.samplingfeatureid > 0

    index_data = {
        "project_ids": [project_response.directiveid],
        "station_uuid": created_sampling_feature.samplingfeatureuuid,
        "date": "2020-09-01T00:00:00.000Z",
        "indices": [{
            "indexType": "PIT EQR",
            "indexValue": "11"
        }]
    }
    response = await post_indices(BegroingIndicesCreate(**index_data), db_conn, USER_HEADER)
    assert response
    # TODO: the endpoint does not really respond with anything worth asserting on


@patch("odm2_postgres_api.routes.begroing_routes.store_begroing_results", autospec=True)
@pytest.mark.asyncio
@pytest.mark.docker
async def test_post_new_begroing_observations(store_begroing_results, db_conn):
    taxon_create = TaxonomicClassifierCreate(
        **{
            "taxonomicclassifiercommonname": "Achnanthes biasolettiana",
            "taxonomicclassifierdescription": "autor:Grun., autor_ref:None↵ph_opt:None, "
            "ph_ref:None↵rubin_kode:ACHN BIA",
            "taxonomicclassifiertypecv": "Biology",
            "taxonomicclassifiername": "ACHN BIA",
        })

    taxon = await post_taxonomic_classifiers(taxon_create, db_conn)

    methods = [
        Methods(
            **{
                "methodid": 3,
                # TODO: this does not match methods in nivabasen
                "methodname": "Microscopic abundance",
                "methoddescription": "A method for observing the abundance of a species in a sample",
                "methodtypecv": "Observation",
                "methodcode": "begroing_1"
            }),
        Methods(
            **{
                "methodcode": "begroing_4",
                "methoddescription": "A quantitative observation is made assessing the abundance of a species in...",
                "methodid": 6,
                "methodlink": None,
                "methodname": "Macroscopic coverage",
                "methodtypecv": "Observation",
            })
    ]

    observations = [
        ["x", ""],
        ["", "<1"],
        ["x", ""],
    ]

    project_create = DirectivesCreate(directivedescription="Test project used in test_post_new_begroing_observations",
                                      directivetypecv="Project")
    project = await post_directive(project_create, db_conn)

    station_create = SamplingFeaturesCreate(
        samplingfeatureuuid=uuid4(),
        samplingfeaturecode="TEST_STATION",  # 'DGL SJU' in begroing
        samplingfeaturetypecv="Site",
        samplingfeaturename="TEST_STATION, Test",
        featuregeometrywkt="POINT (10.791888159215542 59.90455564858938)")
    station = await post_sampling_features(station_create, db_conn)

    begroing_result = BegroingResultCreate(projects=[project],
                                           date=datetime.now(),
                                           station=station,
                                           taxons=[taxon],
                                           methods=methods,
                                           observations=observations)

    await post_begroing_result(begroing_result=begroing_result, connection=db_conn, niva_user=USER_HEADER)

    assert not store_begroing_results.called
