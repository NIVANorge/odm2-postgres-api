import asyncpg


async def get_power_of_2(conn: asyncpg.connection, power):
    return await conn.fetchval('select 2 ^ $1', power)
