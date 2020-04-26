import asyncio
import os
from pathlib import Path

import asyncpg
import pytest
import time
from dotenv import load_dotenv

from odm2_postgres_api.utils.db_initiate import db_init
from test_utils import truncate_all_data


def try_db_init(tries=30):
    try:
        db_init()
    except (asyncpg.exceptions.ConnectionDoesNotExistError, asyncpg.exceptions.CannotConnectNowError):
        if tries != 0:
            time.sleep(.5)
            try_db_init(tries-1)
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
    return await asyncpg.create_pool(user=user, password=password,
                                     host=db_host, port=db_port, database=db_name)


@pytest.fixture(scope="function")
async def clear_db():
    db_pool = await init_dbpool()
    async with db_pool.acquire() as connection:
        await truncate_all_data(connection, "odm2")
        # TODO: move into general metadata (this should be the same in all environments..)
        await connection.fetchrow("INSERT INTO odm2.cv_organizationtype "
                                  "(term, name) "
                                  "VALUES ('researchInstitute', 'Research institute');")
        await connection.fetchrow("INSERT INTO odm2.organizations "
                                  "(organizationtypecv, organizationcode, organizationname) "
                                  "VALUES ('Research institute', 'niva', 'Norwegian institute for Water Research');")
        await connection.fetchrow("INSERT INTO odm2.externalidentifiersystems "
                                  "(identifiersystemorganizationid, externalidentifiersystemname) "
                                  "VALUES ((select organizationid from odm2.organizations where organizationcode='niva'), 'niva-port');")
