from datetime import timezone
from typing import List

from odm2_postgres_api.queries.core_queries import (
    find_row,
    do_action,
    create_feature_action,
    find_or_create_sampling_feature,
)
from odm2_postgres_api.queries.user import StoredPerson
from odm2_postgres_api.routes.fish_rfid.fish_rfid_types import (
    FishObservationResponse,
    FishObservationRequest,
    FishObservationStored,
)
from odm2_postgres_api.schemas.schemas import (
    SamplingFeatures,
    ActionsCreate,
    FeatureActionsCreate,
)


async def register_fish_observations(
    conn, request: FishObservationRequest, user: StoredPerson
) -> FishObservationResponse:
    stored_observations: List[FishObservationStored] = []
    fish_sampling_features: List[SamplingFeatures] = []
    station_sampling_features: List[SamplingFeatures] = []

    for observation in request.observations:
        fish_sf = await find_or_create_sampling_feature(conn, observation.fish_tag, "Specimen")
        # TODO: struggled making set of list of dicts. What is easiest way to ensure uniqueness?
        if fish_sf not in fish_sampling_features:
            fish_sampling_features.append(fish_sf)
        station_sf = await find_row(
            conn,
            "samplingfeatures",
            "samplingfeaturecode",
            observation.station_code,
            SamplingFeatures,
            raise_if_none=True,
        )
        if station_sf not in station_sampling_features:
            station_sampling_features.append(station_sf)

        if observation.datetime.tzinfo:
            obs_time = observation.datetime.astimezone(timezone.utc)
        else:
            # TODO: assuming that date is utc here. should we instead throw an error if tzinfo is not set?
            obs_time = observation.datetime.replace(tzinfo=timezone.utc)
        # TODO: storing action_by as the user object. This is in its current state the user who created the API token
        # we should instead link to equipment used, see https://github.com/NIVANorge/niva-port/issues/226
        action = ActionsCreate(
            actiontypecv="Observation",
            methodcode="fish_rfid:observe_fish",
            affiliationid=user.affiliationid,
            isactionlead=True,
            begindatetime=obs_time,
            begindatetimeutcoffset=0,
            enddatetime=obs_time + observation.duration,
            enddatetimeutcoffset=0,
        )
        stored_action = await do_action(conn, action)

        fa_fish = await create_feature_action(
            conn,
            FeatureActionsCreate(
                samplingfeatureuuid=fish_sf.samplingfeatureuuid,
                actionid=stored_action.actionid,
            ),
        )
        fa_station = await create_feature_action(
            conn,
            FeatureActionsCreate(
                samplingfeatureuuid=station_sf.samplingfeatureuuid,
                actionid=stored_action.actionid,
            ),
        )
        stored_observations.append(
            FishObservationStored(
                action=stored_action,
                fish_sampling_feature=fish_sf.samplingfeatureuuid,
                station_sampling_feature=station_sf.samplingfeatureuuid,
            )
        )

    return FishObservationResponse(
        observations=stored_observations,
        fish_sampling_features=fish_sampling_features,
        station_sampling_features=station_sampling_features,
    )
