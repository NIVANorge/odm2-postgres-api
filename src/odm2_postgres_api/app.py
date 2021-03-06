import os
import logging

import asyncpg

from fastapi import FastAPI
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from nivacloud_logging.log_utils import setup_logging
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.status import HTTP_422_UNPROCESSABLE_ENTITY

from odm2_postgres_api.aquamonitor.aquamonitor_client import AquamonitorAPIError
from odm2_postgres_api.metadata_init.populate_metadata import populate_metadata
from odm2_postgres_api.routes.fish_rfid import fish_rfid_routes
from odm2_postgres_api.utils.api_pool_manager import api_pool_manager

from odm2_postgres_api.routes import (
    begroing_routes,
    shared_routes,
    mass_spectrometry_routes,
)

app = FastAPI(docs_url="/", title="ODM2 API", version="v1")


@app.exception_handler(RequestValidationError)
async def request_validation_exception_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    # Log the error that is send to the client
    logging.debug(f"Body of failed request: {exc.body}")
    logging.info(
        "Error thrown on request",
        extra={"url": request.url, "error_detail": jsonable_encoder(exc.errors())},
    )
    return JSONResponse(
        status_code=HTTP_422_UNPROCESSABLE_ENTITY,
        content={"detail": jsonable_encoder(exc.errors()), "body": exc.body},
    )


@app.exception_handler(AquamonitorAPIError)
async def aquamonitor_api_exception_handler(request: Request, exc: AquamonitorAPIError) -> JSONResponse:
    logging.error(exc, extra={"method": exc.method, "url": exc.url, "status": exc.status_code})
    return JSONResponse(
        status_code=500,
        content={
            "message": "Error connecting to aquamonitor API",
            "detail": exc.message,
        },
    )


@app.on_event("startup")
async def startup_event():
    setup_logging()

    # TODO: This can run before the database is ready, it should actually be lazily tried on the first connection
    # Get DB connection from environment
    db_host = os.environ["TIMESCALE_ODM2_SERVICE_HOST"]
    db_port = os.environ["TIMESCALE_ODM2_SERVICE_PORT"]
    db_mighty_user = os.environ["ODM2_DB_USER"]
    db_mighty_pwd = os.environ["ODM2_DB_PASSWORD"]
    db_name = os.environ["ODM2_DB"]

    logging.info("Creating connection pool")
    api_pool_manager.pool = await asyncpg.create_pool(
        user=db_mighty_user,
        password=db_mighty_pwd,
        server_settings={"search_path": "odm2,public"},
        host=db_host,
        port=db_port,
        database=db_name,
    )
    logging.info("Successfully created connection pool")

    await populate_metadata(api_pool_manager.pool)


@app.on_event("shutdown")
async def shutdown_event():
    logging.info("Closing connection pool")
    await api_pool_manager.pool.close()
    logging.info("Successfully closed connection pool")


app.include_router(shared_routes.router)
app.include_router(
    begroing_routes.router,
    prefix="/begroing",
)
app.include_router(mass_spectrometry_routes.router, prefix="/mass_spec")
app.include_router(fish_rfid_routes.router)
