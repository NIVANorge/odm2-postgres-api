import os
import logging

import asyncpg

from fastapi import FastAPI
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.status import HTTP_422_UNPROCESSABLE_ENTITY
from odm2_postgres_api.utils.api_pool_manager import api_pool_manager

from odm2_postgres_api.routes import begroing_routes, shared_routes


app = FastAPI(
    docs_url="/",
    title="ODM2 API",
    version="v1"
)


@app.exception_handler(RequestValidationError)
async def request_validation_exception_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    # Log the error that is send to the client
    logging.info('Error thrown on request', extra={'url': request.url, 'error_detail': jsonable_encoder(exc.errors())})
    return JSONResponse(
        status_code=HTTP_422_UNPROCESSABLE_ENTITY,
        content={"detail": jsonable_encoder(exc.errors())},
    )


@app.on_event("startup")
async def startup_event():
    # TODO: This can run before the database is ready, it should actually be lazily tried on the first connection
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


app.include_router(begroing_routes.router,   prefix="/begroing",)
app.include_router(shared_routes.router)
