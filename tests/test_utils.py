import json
import logging
from base64 import b64encode
from typing import Dict

from odm2_postgres_api.queries.user import get_nivaport_user, NivaPortUser


async def truncate_all_data(connection, schema: str):
    # TODO: do we need to add some try/except with iterative cascade?
    async def _truncate_tables(schema, tables, iterations=10):
        """
        Iteratively runs truncate table cascade in order to wipe all data
        """
        if iterations < 1:
            raise Exception("Unable to truncate all tables in database")
        truncated_tables = []
        failed = []
        for table in tables:
            # try:
            logging.info(f"Truncating table {table}..")
            await connection.fetch(f"TRUNCATE table {schema}.{table} CASCADE;")
            truncated_tables.append(table)
            logging.info(f"Successfully truncated table {table}")
            # except Exception as e:
            #     failed.insert(table)

        if len(failed) > 0:
            await _truncate_tables(tables=failed, iterations=iterations - 1)

    rows = await connection.fetch("select * from pg_tables where schemaname=$1;", schema)
    tables = [t["tablename"] for t in rows]
    await _truncate_tables(schema=schema, tables=tables)


def test_pool_mamager_uses_same_instance():
    from odm2_postgres_api.routes import begroing_routes, shared_routes
    from odm2_postgres_api.app import api_pool_manager

    assert shared_routes.api_pool_manager is begroing_routes.api_pool_manager
    assert api_pool_manager is begroing_routes.api_pool_manager


def user_header(email="devuser@someemail.com") -> Dict:
    user_obj = {
        "id": 1,
        "uid": "1ed200d3-f09a-4164-9110-a1f24f899bb3",
        "displayName": "Åge Olsen",
        "email": email,
        "provider": "DevLogin",
        "createTime": "2020-04-20T11:45:21.241Z",
        "updateTime": "2020-04-20T11:45:21.241Z",
        "roles": ["apps:admin", "niva"],
    }

    return {"Niva-User": str(b64encode(json.dumps(user_obj).encode("utf-8")), "utf-8")}
