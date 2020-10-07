import os

import pytest
from httpx import AsyncClient

from odm2_postgres_api.aquamonitor.aquamonitor_client import get_method_by_id, get_project_stations, get_taxonomy, \
    get_taxonomy_domain_id, get_taxonomy_codes
from odm2_postgres_api.aquamonitor.aquamonitor_mapping import METHODS_NIVABASE_MAP
"""
Tests that do actual calls to aquamonitor API. Using aquamonitor API in production, 
so we are only doing reading operations
"""


@pytest.fixture(scope="function")
async def aquamonitor_client():
    username = os.environ["AQUAMONITOR_USER"]
    password = os.environ["AQUAMONITOR_PASSWORD"]
    url = "https://test-aquamonitor.niva.no/AquaServices/api"
    async with AsyncClient(base_url=url, auth=(username, password)) as client:
        yield client


@pytest.mark.asyncio
@pytest.mark.aquamonitor_api_test
async def test_aquamonitor_client_get_method(aquamonitor_client):
    method_name = "Kiselalger Relative abundance"
    method_id = METHODS_NIVABASE_MAP[method_name]
    method = await get_method_by_id(aquamonitor_client, method_id=method_id)
    assert method
    assert method.Id == method_id
    assert method.Laboratory == "NIVA"
    assert method.Matrix is None
    assert method.MethodRef is None
    assert method.Name == method_name
    assert method.Unit is None


@pytest.mark.asyncio
@pytest.mark.aquamonitor_api_test
async def test_aquamonitor_client_get_stations(aquamonitor_client):
    station_code = "HEDEGL06"
    station = await get_project_stations(aquamonitor_client,
                                         project_name="Overvåkning av Glomma, Vorma og Øyeren",
                                         station_code=station_code)

    assert station.Name == "Sjulhusbrua, Alvdal"
    assert station.Id == 57692
    assert station.Code == station_code
    assert station.Type["_Text"] == "Elv"


@pytest.mark.asyncio
@pytest.mark.aquamonitor_api_test
async def test_aquamonitor_get_taxonomy(aquamonitor_client):
    taxon_domain = await get_taxonomy_domain_id(aquamonitor_client, "Begroingsalger")
    taxa = await get_taxonomy_codes(aquamonitor_client, domain_id=taxon_domain)
    bambusina_spp = [t for t in taxa if t.Code == "BAMBUSIZ"][0]
    assert bambusina_spp.Id

    taxon = await get_taxonomy(aquamonitor_client, domain_name="Begroingsalger", code=bambusina_spp.Code)

    # comparing taxon fetched from get_taxonomy and get_taxonomy_codes. Should be equal
    assert taxon.Id == bambusina_spp.Id
    assert taxon.Code == bambusina_spp.Code
    assert taxon.Name == bambusina_spp.Name
    # TODO: would like to assert that the two objects are equal, but they do indeed differ on domain. This fails:
    # assert taxon.Domain == bambusina_spp.Domain
    # assert taxon.Taxonomy == bambusina_spp.Taxonomy
