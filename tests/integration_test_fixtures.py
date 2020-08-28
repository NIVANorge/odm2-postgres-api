import asyncio
import logging
import os
from pathlib import Path

import asyncpg
import pytest
import time
from dotenv import load_dotenv

from odm2_postgres_api.db_init.db_initiate import db_init
from odm2_postgres_api.metadata_init.populate_metadata import populate_metadata
from test_utils import truncate_all_data


def try_db_init(tries=30):
    try:
        db_init()
    except (asyncpg.exceptions.ConnectionDoesNotExistError, asyncpg.exceptions.CannotConnectNowError):
        if tries != 0:
            time.sleep(.5)
            logging.info("waiting..")
            try_db_init(tries - 1)
        else:
            raise


@pytest.fixture(scope="module")
def wait_for_db(module_scoped_container_getter):
    db_info = module_scoped_container_getter.get('timescale_odm2').network_info[0]
    env_file = Path(__file__).parent / '..' / 'src' / 'odm2_postgres_api' / 'config' / 'localdocker.env'
    load_dotenv(dotenv_path=env_file, verbose=True)

    os.environ["TIMESCALE_ODM2_SERVICE_HOST"] = db_info.hostname
    os.environ["TIMESCALE_ODM2_SERVICE_PORT"] = db_info.host_port
    try_db_init()

    asyncio.set_event_loop(asyncio.new_event_loop())


async def init_dbpool():
    db_host = os.environ["TIMESCALE_ODM2_SERVICE_HOST"]
    db_port = os.environ["TIMESCALE_ODM2_SERVICE_PORT"]
    user = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]
    db_name = os.environ["ODM2_DB"]
    return await asyncpg.create_pool(user=user, password=password, server_settings={"search_path": "odm2"},
                                     host=db_host, port=db_port, database=db_name)


@pytest.fixture(scope="function")
async def clear_db():
    db_pool = await init_dbpool()
    async with db_pool.acquire() as connection:
        await truncate_all_data(connection, "odm2")
    await populate_metadata(db_pool)
