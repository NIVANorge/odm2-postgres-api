from uuid import UUID
from datetime import datetime
from typing import Dict, List

from asyncpg import connection

from odm2_postgres_api.queries.core_queries import find_row
from odm2_postgres_api.schemas.schemas import (
    BegroingObservations,
    Directive,
    SamplingFeatures,
    BegroingObservationValues,
    TaxonomicClassifier,
    Methods,
    BegroingObservation,
)


async def find_begroing_results(
    conn: connection,
    project_id: int,
    sampling_feature_uuid: UUID,
    start_time: datetime,
    end_time: datetime,
) -> List[BegroingObservation]:
    def observations_from_row(station: SamplingFeatures, project: Directive, row: Dict) -> BegroingObservation:
        taxon = TaxonomicClassifier(**row)
        method = Methods(**row)
        value = row["categorical_value"] or str(row["measurement_value"])
        return BegroingObservation(
            project=project, station=station, timestamp=row["resultdatetime"], taxon=taxon, method=method, value=value
        )

    project: Directive = await find_row(conn, "directives", "directiveid", project_id, Directive, raise_if_none=True)
    sampling_feature: SamplingFeatures = await find_row(
        conn, "samplingfeatures", "samplingfeatureuuid", sampling_feature_uuid, SamplingFeatures, raise_if_none=True
    )

    rows = await conn.fetch(
        """
select crv.datavalue as categorical_value,
       mrv.datavalue as measurement_value,
       r.resultdatetime,
       t.*,
       m.*,
       aff.primaryemail
from results r
         full outer join categoricalresultvalues crv on crv.resultid = r.resultid
         full outer join measurementresultvalues mrv on mrv.resultid = r.resultid
         inner join taxonomicclassifiers t on t.taxonomicclassifierid = r.taxonomicclassifierid
         inner join featureactions fa on r.featureactionid = fa.featureactionid
         inner join actions a on fa.actionid = a.actionid
         inner join methods m on a.methodid = m.methodid
         inner join actiondirectives ad on ad.actionid = a.actionid
         inner join actionby ab on ab.actionid = a.actionid
         inner join affiliations aff on ab.affiliationid = aff.affiliationid
         inner join samplingfeatures s on fa.samplingfeatureid = s.samplingfeatureid
where
    fa.samplingfeatureid = $1 and
    ad.directiveid = $2 and
    r.resultdatetime>=$3 and
    r.resultdatetime<=$4
    """,
        sampling_feature.samplingfeatureid,
        project_id,
        start_time,
        end_time,
    )

    return [observations_from_row(station=sampling_feature, project=project, row=r) for r in rows]
