import os
import logging
import asyncio
from pathlib import Path

import asyncpg
from dotenv import load_dotenv
from sqlalchemy import create_engine

from nivacloud_logging.log_utils import setup_logging

# This SQLAlchemy engine is purely used for escaping strings
ENGINE = create_engine('postgresql://user:password@nohost')


def quoter(word):
    return ENGINE.dialect.preparer(ENGINE.dialect).quote(word)


async def create_database_if_not_exists(conn, quoted_db_name: str):
    if not await conn.fetchval(f"SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname = $1", quoted_db_name) == 1:
        logging.info(await conn.execute(f"CREATE DATABASE {quoted_db_name}"))
    else:
        logging.info("Database exists, doing nothing", extra={'database': quoted_db_name})


async def create_user_if_not_exists(conn, credentials: dict):
    # password cannot be run through qouter because it is unpredictable if it gets qouted or not...
    user = quoter(credentials['user_name'])
    password = credentials['password']

    if await conn.fetchval(f"SELECT count(*) FROM pg_catalog.pg_roles WHERE rolname = $1", user) != 0:
        logging.info(f"User/role exists, doing nothing", extra={'db_user': user})
    else:
        if password:
            logging.info(await conn.execute(f"CREATE ROLE {user} WITH LOGIN ENCRYPTED PASSWORD '{password}'"))
        else:
            logging.info(await conn.execute(f"CREATE ROLE {user}"))


async def grant_all_on_db_to_role(conn, quoted_schema: str, quoted_mighty_user: str):
    commands = [
        f"GRANT USAGE ON SCHEMA {quoted_schema} TO {quoted_mighty_user}",
        f"GRANT ALL ON ALL TABLES IN SCHEMA {quoted_schema} TO {quoted_mighty_user}",
        f"GRANT ALL ON ALL SEQUENCES IN SCHEMA {quoted_schema} TO {quoted_mighty_user}",
    ]
    for command in commands:
        logging.info(await conn.execute(command))


async def grant_read_only(conn, quoted_schema, db_name, read_only_user, granting_user):
    commands = [
        f"REVOKE ALL ON DATABASE {db_name} FROM {read_only_user}",
        f"GRANT CONNECT ON DATABASE {db_name} TO {read_only_user}",
        f"GRANT USAGE ON SCHEMA {quoted_schema} TO {read_only_user}",
        f"GRANT SELECT ON ALL TABLES IN SCHEMA {quoted_schema}  TO {read_only_user}",
        f"ALTER DEFAULT PRIVILEGES FOR ROLE {granting_user} IN SCHEMA {quoted_schema} "
        f"GRANT SELECT ON TABLES TO {read_only_user}",
    ]
    for command in commands:
        logging.info(await conn.execute(command))


async def postgres_user_on_postgres_db(connection_string, new_db: str, db_users: dict):
    conn = await asyncpg.connect(connection_string)
    try:
        await create_database_if_not_exists(conn, quoter(new_db))
        async with conn.transaction():
            await create_user_if_not_exists(conn, db_users['odm2_owner'])
            await create_user_if_not_exists(conn, db_users['read_only_user'])
    finally:
        await conn.close()


async def postgres_user_on_odm_db(connection_string, db_name: str, odm2_schema_name: str, users: dict):
    quoted_schema = quoter(odm2_schema_name)
    quoted_mighty_user = quoter(users['odm2_owner']['user_name'])
    quoted_read_only_user = quoter(users['read_only_user']['user_name'])
    conn = await asyncpg.connect(connection_string)
    try:
        async with conn.transaction():
            if await conn.fetchval(f"SELECT count(*) FROM pg_namespace where nspname like '{quoted_schema}'") != 1:
                logging.info(await conn.execute(f"CREATE SCHEMA {quoted_schema} AUTHORIZATION {quoted_mighty_user}"))
            commands = [
                f"ALTER ROLE {quoted_mighty_user} IN DATABASE {db_name} SET search_path = {quoted_schema},public",
                f"ALTER ROLE {quoted_read_only_user} IN DATABASE {db_name} SET search_path = {quoted_schema},public",
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
                if command and '--' not in command:
                    try:
                        logging.info(await conn.execute(command))
                    except (asyncpg.exceptions.DuplicateObjectError, asyncpg.exceptions.DuplicateTableError):
                        failed_commands += 1
        logging.info(f'Duplicate commands from ODM2_for_PostgreSQL.sql: {failed_commands}')
        await run_create_hypertable_commands(connection_string)
        async with conn.transaction():
            await grant_all_on_db_to_role(conn, quoted_schema, quoted_mighty_user)
            await grant_read_only(conn, quoted_schema, db_name, quoted_read_only_user, quoted_mighty_user)
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


def main():
    setup_logging(plaintext=True)
    if os.environ.get('NIVA_ENVIRONMENT') not in ['dev', 'master']:
        if Path.cwd() == Path('/app'):
            env_file = Path(__file__).parent / '..' / 'config' / 'localdocker.env'
        else:
            env_file = Path(__file__).parent / '..' / 'config' / 'localdev.env'
        load_dotenv(dotenv_path=env_file, verbose=True)

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
    asyncio.run(postgres_user_on_postgres_db(connection_string, db_name, db_users))

    # Run commands on new database
    connection_string = f'postgresql://{pg_credentials}@{db_host}:{db_port}/{db_name}'
    asyncio.run(postgres_user_on_odm_db(connection_string, db_name, odm2_schema_name, db_users))

    logging.info("Database tables for odm2 created")


if __name__ == '__main__':
    main()
