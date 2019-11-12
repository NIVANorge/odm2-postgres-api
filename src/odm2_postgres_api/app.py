import os
import logging
import asyncpg

from fastapi import FastAPI, Depends

from odm2_postgres_api.queries import get_power_of_2

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

    api_pool_manager.pool = await asyncpg.create_pool(user=db_mighty_user, password=db_mighty_pwd,
                                                      host=db_host, port=db_port, database=db_name)


@app.on_event("shutdown")
async def shutdown_event():
    logging.info('Closing connection pool')
    await api_pool_manager.pool.close()


@app.get("/hello", summary="api 101 testing")
async def hello(connection=Depends(api_pool_manager.get_conn)):
    async with connection.transaction():
        result = await get_power_of_2(connection, 4)
        logging.info(result)
        return {"message": f"Hello World {result}"}


@app.get("/make_con", summary="api 101 testing")
async def make_con():
    return {"message": "connection is made"}
