import os
import logging
import asyncpg

from fastapi import FastAPI, Depends

from odm2_postgres_api.schemas import schemas
from odm2_postgres_api.queries import core_queries
from odm2_postgres_api.queries.controlled_vocabulary_queries import synchronize_cv_tables

app = FastAPI(
    docs_url="/",
    title="ODM2 API",
    version="v1"
)


class ApiPoolManager:
    def __init__(self):
        self.pool = None  # Optional[asyncpg.pool.Pool]

    async def get_conn(self) -> asyncpg.connection.Connection:
        async with self.pool.acquire() as connection:
            yield connection


api_pool_manager = ApiPoolManager()


@app.on_event("startup")
async def startup_event():
    # Get DB connection from environment
    db_host = os.environ["TIMESCALE_ODM2_SERVICE_HOST"]
    db_port = os.environ["TIMESCALE_ODM2_SERVICE_PORT"]
    db_mighty_user = os.environ["ODM2_DB_USER"]
    db_mighty_pwd = os.environ["ODM2_DB_PASSWORD"]
    db_name = os.environ["ODM2_DB"]

    logging.info("Creating connection pool")
    api_pool_manager.pool = await asyncpg.create_pool(user=db_mighty_user, password=db_mighty_pwd,
                                                      host=db_host, port=db_port, database=db_name)
    logging.info("Successfully created connection pool")


@app.on_event("shutdown")
async def shutdown_event():
    logging.info('Closing connection pool')
    await api_pool_manager.pool.close()
    logging.info('Successfully closed connection pool')


@app.patch("/controlled_vocabularies", summary="api 101 testing")
async def patch_controlled_vocabularies():
    return await synchronize_cv_tables(api_pool_manager.pool)


@app.post("/controlled_vocabularies/{controlled_vocabulary}", summary="api 101 testing")
async def post_controlled_vocabularies(controlled_vocabulary: schemas.cv_checker):  # type: ignore
    return await synchronize_cv_tables(api_pool_manager.pool, [controlled_vocabulary])


@app.post("/people", response_model=schemas.People)
async def post_people(user: schemas.PeopleCreate, connection=Depends(api_pool_manager.get_conn)):
    return await core_queries.create_person(connection, user)


@app.post("/organizations", response_model=schemas.Organizations)
async def post_organizations(user: schemas.OrganizationsCreate, connection=Depends(api_pool_manager.get_conn)):
    return await core_queries.create_organization(connection, user)
