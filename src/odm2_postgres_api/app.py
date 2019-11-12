import os
import logging

from fastapi import FastAPI
from odm2_postgres_api.ORM.models import db

app = FastAPI(
    docs_url="/",
    title="ODM2 API",
    version="v1"
)


@app.on_event("startup")
async def startup_event():
    # Get DB connection from environment
    db_host = os.environ["TIMESCALE_ODM2_SERVICE_HOST"]
    db_port = os.environ["TIMESCALE_ODM2_SERVICE_PORT"]
    db_mighty_user = os.environ["ODM2_DB_USER"]
    db_mighty_pwd = os.environ["ODM2_DB_PASSWORD"]
    db_name = os.environ["ODM2_DB"]

    logging.info(f'postgresql://{db_mighty_user}:{db_mighty_pwd}@{db_host}:{db_port}/{db_name}')
    await db.set_bind(f'postgresql://{db_mighty_user}:{db_mighty_pwd}@{db_host}:{db_port}/{db_name}')
    await db.gino.create_all()
    await db.status(db.text("SELECT create_hypertable('odm2.TimeSeriesResultValues', 'valueid', "
                            "chunk_time_interval => 100000)"))
    # SELECT create_hypertable('odm2.TimeSeriesResultValues', 'valueid', chunk_time_interval => 100000);


@app.on_event("shutdown")
async def shutdown_event():
    await db.pop_bind().close()


@app.get("/hello", summary="api 101 testing")
async def root():
    async with db.acquire() as conn:
        async with conn.transaction() as tx:
            return {"message": "Hello World"}


@app.get("/make_con", summary="api 101 testing")
async def root():
    return {"message": "connection is made"}
