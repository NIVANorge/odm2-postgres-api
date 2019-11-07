from fastapi import FastAPI
from gino import Gino

app = FastAPI(
    docs_url="/",
    title="ODM2 API",
    version="v1"
)
db = Gino()


@app.on_event("startup")
async def startup_event():
    await db.set_bind('postgresql://postgres:postgres@timescaledb:5432/niva_odm2')


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
