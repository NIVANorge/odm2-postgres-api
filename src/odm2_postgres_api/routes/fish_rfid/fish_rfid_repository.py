from typing import List
from uuid import uuid4

from odm2_postgres_api.queries.core_queries import find_row, insert_pydantic_object, do_action, create_feature_action
from odm2_postgres_api.queries.user import StoredPerson
from odm2_postgres_api.routes.fish_rfid.fish_rfid_types import FishObservationCreate, FishObservationResponse, \
    FishObservationRequest, FishObservationStored
from odm2_postgres_api.schemas.schemas import SamplingFeatures, SamplingFeaturesCreate, ActionsCreate, \
    FeatureActionsCreate, ActionsByCreate, ActionsByFields


async def find_or_create_sampling_feature(
        conn, observation: FishObservationCreate
) -> SamplingFeatures:
    existing = await find_row(
        conn,
        "samplingfeatures",
        "samplingfeaturecode",
        observation.fish_tag,
        SamplingFeatures,
    )

    if existing:
        return existing

    new_sf = SamplingFeaturesCreate(
        samplingfeatureuuid=uuid4(),
        samplingfeaturetypecv="Specimen",
        samplingfeaturecode=observation.fish_tag,
    )

    return await insert_pydantic_object(
        conn, "samplingfeatures", new_sf, SamplingFeatures
    )


async def register_fish_observations(conn, request: FishObservationRequest,
                                     user: StoredPerson) -> FishObservationResponse:
    stored_observations: List[FishObservationStored] = []
    for observation in request.observations:
        fish_sf = await find_or_create_sampling_feature(conn, observation)
        station_sf = await find_row(conn, "samplingfeatures", "samplingfeaturecode", observation.station_code,
                                    SamplingFeatures)

        # TODO: storing action_by. This is in its current state the user who created the API token
        # we should instead link to equipment used, see https://github.com/NIVANorge/niva-port/issues/226
        action = ActionsCreate(actiontypecv="Observation", methodcode="fish_rfid:observe_fish",
                               begindatetime=observation.datetime, begindatetimeutcoffset=0,
                               enddatetime=observation.datetime + observation.duration, enddatetimeutcoffset=0)
        action = await do_action(conn, action)

        fa_fish = await create_feature_action(conn,
                                              FeatureActionsCreate(samplingfeatureuuid=fish_sf.samplingfeatureuuid,
                                                                   actionid=action.actionid))
        fa_station = await create_feature_action(conn, FeatureActionsCreate(
            samplingfeatureuuid=station_sf.samplingfeatureuuid,
            actionid=action.actionid))
        stored_observations.append(FishObservationStored(action=action, fish_sampling_feature=fish_sf,
                                                         station_sampling_feature=station_sf))

    return FishObservationResponse(observations=stored_observations)
