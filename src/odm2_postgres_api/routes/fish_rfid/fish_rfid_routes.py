from fastapi import APIRouter, Header, Depends

from odm2_postgres_api.queries.user import create_or_get_user
from odm2_postgres_api.routes.fish_rfid.fish_rfid_repository import register_fish_observations
from odm2_postgres_api.routes.fish_rfid.fish_rfid_types import FishObservationRequest, FishObservationResponse
from odm2_postgres_api.utils.api_pool_manager import api_pool_manager

router = APIRouter()


@router.post("/fish-rfid/observation", response_model=FishObservationResponse)
async def post_fish_observation(
        request: FishObservationRequest,
        conn=Depends(api_pool_manager.get_conn),
        niva_user: str = Header(None),
):
    async with conn.transaction():
        user = await create_or_get_user(conn, niva_user)
        return await register_fish_observations(conn, request, user)
