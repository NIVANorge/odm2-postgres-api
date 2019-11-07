import os
import logging
import asyncio
from pathlib import Path

from gino import Gino
from dotenv import load_dotenv
from ODM2.src.load_cvs import cvload
from asyncpg.exceptions import DuplicateTableError, DuplicateObjectError

from nivacloud_logging.log_utils import setup_logging

db = Gino()


def quoter(engine, word):
    return engine.dialect.preparer(engine.dialect).quote(word)


async def create_database_if_not_exists(quoted_db_name):
    sql_query = db.text(f"SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname = :db")
    if not await db.scalar(sql_query, {'db': quoted_db_name}) == 1:
        logging.info(await db.status(db.text(f"CREATE DATABASE {quoted_db_name}")))
    else:
        logging.info("Database exists, doing nothing", extra={'database': quoted_db_name})


async def create_user_if_not_exists(user, password=None):
    user_count_query = db.text('SELECT count(*) FROM pg_catalog.pg_roles WHERE rolname = :usr')
    if await db.scalar(user_count_query, {'usr': user}) != 0:
        logging.info(f"User/role exists, doing nothing", extra={'db_user': user})
    else:
        if password:
            logging.info(await db.status(db.text(f"CREATE ROLE {user} WITH LOGIN ENCRYPTED PASSWORD '{password}'")))
        else:
            logging.info(await db.status(db.text(f"CREATE ROLE {user}")))


async def grant_all_on_db_to_role(quoted_schema, quoted_mighty_user):
    commands = [
        f"GRANT USAGE ON SCHEMA {quoted_schema} TO {quoted_mighty_user}",
        f"GRANT ALL ON ALL TABLES IN SCHEMA {quoted_schema} TO {quoted_mighty_user}",
        f"GRANT ALL ON ALL SEQUENCES IN SCHEMA {quoted_schema} TO {quoted_mighty_user}",
    ]
    for command in commands:
        logging.info(await db.status(db.text(command)))


async def postgres_user_on_postgres_db(connection_string, new_db, db_mighty_user, db_mighty_pwd):
    async with db.with_bind(connection_string) as engine:
        await create_database_if_not_exists(quoter(engine, new_db))
        await create_user_if_not_exists(quoter(engine, db_mighty_user), quoter(engine, db_mighty_pwd))


async def postgres_user_on_odm_db(connection_string, odm2_location, odm2_schema_name, db_mighty_user):
    async with db.with_bind(connection_string) as engine:
        quoted_schema = quoter(engine, odm2_schema_name)
        quoted_mighty_user = quoter(engine, db_mighty_user)

        schema_count_query = db.text(f"SELECT count(*) FROM pg_namespace where nspname like '{quoted_schema}'")
        if await db.scalar(schema_count_query) != 1:
            logging.info(await db.status(db.text(f"CREATE SCHEMA {quoted_schema} AUTHORIZATION {quoted_mighty_user}")))
            command = f"ALTER ROLE {quoted_mighty_user} IN DATABASE niva_odm2 SET search_path = {quoted_schema};"
            logging.info(await db.status(db.text(command)))

        extensions = ['timescaledb', 'postgis', 'postgis_topology', 'fuzzystrmatch', 'postgis_tiger_geoCoder']
        for extension in extensions:
            logging.info(await db.status(db.text(f'CREATE EXTENSION IF NOT EXISTS {extension} CASCADE')))

        with open(odm2_location / 'src' / 'blank_schema_scripts' / 'postgresql' / 'ODM2_for_PostgreSQL.sql') as my_file:
            postgres_odm2_code = my_file.read()
        for command in postgres_odm2_code.split(';'):
            if command and '--' not in command:
                try:
                    logging.info(await db.status(db.text(command)))
                except (DuplicateTableError, DuplicateObjectError):
                    pass
        await grant_all_on_db_to_role(quoted_schema, quoted_mighty_user)


async def odm_user_on_odm_db(connection_string):
    async with db.with_bind(connection_string) as engine:
        logging.info("Database tables for odm2 created")
        hypertable_sql = ["CREATE ",
                          "SELECT create_hypertable('ts', 'time', 'uuid', 1, if_not_exists=>TRUE)",
                          "SELECT create_hypertable('flag', 'time', 'uuid', 1, if_not_exists=>TRUE)",
                          "SELECT create_hypertable('track', 'time', 'uuid', 1, if_not_exists=>TRUE)"]
        hypertable_sql = []
        for command in hypertable_sql:
            logging.info(await db.status(db.text(command)))


def main():
    setup_logging(plaintext=True)
    if Path.cwd() == Path('/app'):
        env_file = Path(__file__).parent / '..' / 'config' / 'localdocker.env'
        odm2_location = Path(__file__).parent / 'ODM2'
    else:
        env_file = Path(__file__).parent / '..' / 'config' / 'localdev.env'
        odm2_location = Path(os.getcwd()) / '..' / '..' / '..' / '..' / 'ODM2'
    load_dotenv(dotenv_path=env_file, verbose=True)

    # Get DB connection from environment
    db_host = os.environ["TIMESCALE_ODM2_SERVICE_HOST"]
    db_port = os.environ["TIMESCALE_ODM2_SERVICE_PORT"]
    db_mighty_user = os.environ["ODM2_DB_USER"]
    db_mighty_pwd = os.environ["ODM2_DB_PASSWORD"]
    odm2_schema_name = os.environ["ODM2_SCHEMA_NAME"]
    db_name = os.environ["ODM2_DB"]
    pg_user = os.environ["POSTGRES_USER"]
    pg_pwd = os.environ["POSTGRES_PASSWORD"]

    connection_string = f'postgresql://{pg_user}:{pg_pwd}@{db_host}:{db_port}'
    asyncio.run(postgres_user_on_postgres_db(connection_string, db_name, db_mighty_user, db_mighty_pwd))

    # Run commands on new database
    connection_string = f'postgresql://{pg_user}:{pg_pwd}@{db_host}:{db_port}/{db_name}'
    asyncio.run(postgres_user_on_odm_db(connection_string, odm2_location, odm2_schema_name, db_mighty_user))

    connection_string = f'postgresql://{db_mighty_user}:{db_mighty_pwd}@{db_host}:{db_port}/{db_name}'
    asyncio.run(odm_user_on_odm_db(connection_string))

    # Load controlled vocabularies
    cvload.load_controlled_vocabularies(connection_string)


if __name__ == '__main__':
    main()
