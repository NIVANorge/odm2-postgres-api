import shapely.geometry
import shapely.wkb


def encode_geometry(geometry):
    if not hasattr(geometry, '__geo_interface__'):
        raise TypeError('{g} does not conform to ' 'the geo interface'.format(g=geometry))
    shape = shapely.geometry.asShape(geometry)
    return shapely.wkb.dumps(shape)


def decode_geometry(wkb):
    return shapely.wkb.loads(wkb)


async def set_shapely_adapter(connection):
    """From https://magicstack.github.io/asyncpg/current/usage.html#example-automatic-conversion-of-postgis-types"""
    # TODO: somehow implement this: https://stackoverflow.com/questions/29500460/
    #  why-can-shapely-geos-parse-this-invalid-well-known-binary/29548439#29548439
    await connection.set_type_codec(
        'geometry',  # also works for 'geography'
        encoder=encode_geometry,
        decoder=decode_geometry,
        format='binary',
    )
