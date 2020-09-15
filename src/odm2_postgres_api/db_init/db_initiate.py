import os
import logging
import asyncio
from pathlib import Path

import asyncpg
from dotenv import load_dotenv
from nivacloud_logging.log_utils import setup_logging


async def create_database_if_not_exists(conn, quoted_db_name: str):
    if not await conn.fetchval(f"SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname = $1", quoted_db_name) == 1:
        logging.info(await conn.execute(f"CREATE DATABASE {quoted_db_name}"))
    else:
        logging.info("Database exists, doing nothing", extra={'database': quoted_db_name})


async def create_user_if_not_exists(conn, credentials: dict):
    user, password = credentials['user_name'], credentials['password']

    if await conn.fetchval(f"SELECT count(*) FROM pg_catalog.pg_roles WHERE rolname = $1", user) != 0:
        logging.info(f"User/role exists, doing nothing", extra={'db_user': user})
    else:
        logging.info(await conn.execute(f"CREATE ROLE {user} WITH LOGIN ENCRYPTED PASSWORD '{password}'"))


async def grant_all_on_db_to_role(conn, schema: str, owning_user: str):
    commands = [
        f"GRANT USAGE ON SCHEMA {schema} TO {owning_user}",
        f"GRANT ALL ON ALL TABLES IN SCHEMA {schema} TO {owning_user}",
        f"GRANT ALL ON ALL SEQUENCES IN SCHEMA {schema} TO {owning_user}",
    ]
    for command in commands:
        logging.info(await conn.execute(command))


async def grant_read_only(conn, schema, db_name, read_only_user, granting_user):
    commands = [
        f"REVOKE ALL ON DATABASE {db_name} FROM {read_only_user}",
        f"GRANT CONNECT ON DATABASE {db_name} TO {read_only_user}",
        f"GRANT USAGE ON SCHEMA {schema} TO {read_only_user}",
        f"GRANT SELECT ON ALL TABLES IN SCHEMA {schema}  TO {read_only_user}",
        f"ALTER DEFAULT PRIVILEGES FOR ROLE {granting_user} IN SCHEMA {schema} "
        f"GRANT SELECT ON TABLES TO {read_only_user}",
    ]
    for command in commands:
        logging.info(await conn.execute(command))


async def postgres_user_on_postgres_db(connection_string, db_name: str, db_users: dict):
    conn = await asyncpg.connect(connection_string)
    try:
        await create_database_if_not_exists(conn, db_name)
        await create_user_if_not_exists(conn, db_users['odm2_owner'])
        await create_user_if_not_exists(conn, db_users['read_only_user'])
    finally:
        await conn.close()


async def postgres_user_on_odm_db(connection_string, db_name: str, schema_name: str, users: dict):
    owning_user, read_only_user = users['odm2_owner']['user_name'], users['read_only_user']['user_name']
    conn = await asyncpg.connect(connection_string)
    try:
        async with conn.transaction():
            if await conn.fetchval(f"SELECT count(*) FROM pg_namespace where nspname like '{schema_name}'") != 1:
                logging.info(await conn.execute(f"CREATE SCHEMA {schema_name} AUTHORIZATION {owning_user}"))
            commands = [
                f"ALTER ROLE {owning_user} IN DATABASE {db_name} SET search_path = {schema_name},public",
                f"ALTER ROLE {read_only_user} IN DATABASE {db_name} SET search_path = {schema_name},public",
            ]
            for command in commands:
                logging.info(await conn.execute(command))
            extensions = ['timescaledb', 'postgis', 'postgis_topology', 'fuzzystrmatch', 'postgis_tiger_geoCoder']
            for extension in extensions:
                res = await conn.execute(f'CREATE EXTENSION IF NOT EXISTS {extension} CASCADE')
                logging.info(res)
        failed_commands = 0
        with open(Path(__file__).parent / 'ODM2_for_PostgreSQL.sql') as my_file:
            postgres_odm2_code = my_file.read()
        for command in postgres_odm2_code.split(';'):
            async with conn.transaction():
                if command and '--' not in command and command is not '\n':
                    try:
                        logging.info(await conn.execute(command))
                    except (asyncpg.exceptions.DuplicateObjectError,
                            asyncpg.exceptions.DuplicateTableError,
                            asyncpg.exceptions.DuplicateColumnError):
                        failed_commands += 1
        logging.info(f'Duplicate commands from ODM2_for_PostgreSQL.sql: {failed_commands}')
        await run_create_hypertable_commands(connection_string)
        async with conn.transaction():
            await grant_all_on_db_to_role(conn, schema_name, owning_user)
            await grant_read_only(conn, schema_name, db_name, read_only_user, owning_user)
    finally:
        await conn.close()


async def run_create_hypertable_commands(connection_string):
    hyper_tables_sql = [
        # "SELECT create_hypertable('odm2.TimeSeriesResultValues', 'valueid', chunk_time_interval => 100000)",
        "SELECT create_hypertable('ODM2.TrackResultLocations', 'valuedatetime', "
        "chunk_time_interval => interval '7 day', if_not_exists=>TRUE)",
        "SELECT create_hypertable('ODM2.TrackResultValues', 'valuedatetime', "
        "chunk_time_interval => interval '7 day', if_not_exists=>TRUE);"
    ]

    conn = await asyncpg.connect(connection_string)
    try:
        async with conn.transaction():
            for command in hyper_tables_sql:
                logging.info(await conn.execute(command))
    finally:
        await conn.close()


async def wait_for_db_ready(connection_string: str, attempts=20):
    try:
        logging.info(f"Attempting to connect to postgres db")
        async with asyncpg.connect(connection_string) as connection:
            logging.info("Connected to database")
    except Exception as e:
        logging.warning(e)
        await asyncio.sleep(0.5)
        if attempts == 0:
            raise e
        logging.info(f"DB not ready yet, sleeping. ", extra={"attempts_left": attempts})
        await wait_for_db_ready(connection_string, attempts=attempts - 1)


def db_init():
    # Get DB connection from environment
    db_host = os.environ["TIMESCALE_ODM2_SERVICE_HOST"]
    db_port = os.environ["TIMESCALE_ODM2_SERVICE_PORT"]
    db_users = {'postgres_owner': {'user_name': os.environ["POSTGRES_USER"],
                                   'password': os.environ["POSTGRES_PASSWORD"]},
                'odm2_owner': {'user_name': os.environ["ODM2_DB_USER"],
                               'password': os.environ["ODM2_DB_PASSWORD"]},
                'read_only_user': {'user_name': os.environ["ODM2_DB_READ_ONLY_USER"],
                                   'password': os.environ["ODM2_DB_READ_ONLY_PASSWORD"]}}

    db_name = os.environ["ODM2_DB"]
    odm2_schema_name = os.environ["ODM2_SCHEMA_NAME"]

    pg_credentials = f"{db_users['postgres_owner']['user_name']}:{db_users['postgres_owner']['password']}"
    connection_string = f'postgresql://{pg_credentials}@{db_host}:{db_port}'

    asyncio.run(wait_for_db_ready(connection_string))
    asyncio.run(postgres_user_on_postgres_db(connection_string, db_name, db_users))

    # Run commands on new database
    connection_string = f'postgresql://{pg_credentials}@{db_host}:{db_port}/{db_name}'
    asyncio.run(postgres_user_on_odm_db(connection_string, db_name, odm2_schema_name, db_users))

    logging.info("Database tables for odm2 created")


if __name__ == '__main__':
    setup_logging(plaintext=True)
    if os.environ.get('NIVA_ENVIRONMENT') not in ['dev', 'master']:
        if Path.cwd() == Path('/app'):
            env_file = Path(__file__).parent / '..' / 'config' / 'localdocker.env'
        else:
            env_file = Path(__file__).parent / '..' / 'config' / 'localdev.env'
        load_dotenv(dotenv_path=env_file, verbose=True)
    db_init()
