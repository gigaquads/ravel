from appyratus.enum import Enum

from ravel.schema import Field
from ravel.resolver.resolvers.loader import Loader, LoaderProperty
from ravel.query.predicate import Predicate, ConditionalPredicate


SUPPORTED_GEOMETRY_TYPES = Enum(
    POINT='POINT',
    POLYGON='POLYGON',
)

POSTGIS_OP_CODE = Enum(
    CONTAINS='contains',
    CONTAINED_BY='contained-by',
    WITHIN_RADIUS='within-radius',
)


class GeometryObject:
    def __init__(self, geometry_type):
        self.geometry_type = geometry_type

    def to_EWKT_string(self) -> str:
        raise NotImplementedError('override in subclass')


class PointGeometry(GeometryObject):
    def __init__(self, obj):
        if isinstance(obj, PointGeometry):
            vertex = obj.vertex
        elif isinstance(obj, dict):
            vertex = obj['vertex']
        else:
            vertex = obj

        super().__init__(SUPPORTED_GEOMETRY_TYPES.POINT)
        self.vertex = vertex

    def __getitem__(self, idx):
        return self.vertex[idx]

    def to_EWKT_string(self) -> str:
        x, y = self.vertex
        return f'SRID=4326; POINT({x} {y})'


class PolygonGeometry(GeometryObject):
    def __init__(self, obj):
        if isinstance(obj, PolygonGeometry):
            vertices = obj.vertices
        elif isinstance(obj, dict):
            vertices = obj['vertices']
        else:
            vertices = obj

        super().__init__(SUPPORTED_GEOMETRY_TYPES.POLYGON)
        self.vertices = vertices

    def to_EWKT_string(self) -> str:
        points = [f'{v[0]} {v[1]}' for v in self.vertices]
        return f'SRID=4326; POLYGON(({ ",".join(points) }))'


class PostgisGeometryLoaderProperty(LoaderProperty):
    def contains(self, geometry: 'GeometryObject') -> Predicate:
        return ConditionalPredicate(POSTGIS_OP_CODE.CONTAINS, self, geometry)

    def is_within_radius_of(self, point, radius):
        value = {'center': PointGeometry(point).vertex, 'radius': radius}
        return ConditionalPredicate(
            POSTGIS_OP_CODE.WITHIN_RADIUS, self, value,
            ignore_field_adapter=True
        )

    def is_contained_by(self, polygon: 'PolygonGeometry') -> Predicate:
        return ConditionalPredicate(
            POSTGIS_OP_CODE.CONTAINED_BY, self, polygon
        )


class PostgisGeometryLoader(Loader):

    @classmethod
    def property_type(cls):
        return PostgisGeometryLoaderProperty


class Geometry(Field):
    def __init__(self, geo_type, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.meta['resolver_type'] = PostgisGeometryLoader
        self.geo_type = geo_type


class Point(Geometry):
    def __init__(self, *args, **kwargs):
        super().__init__(SUPPORTED_GEOMETRY_TYPES.POINT, *args, **kwargs)

    def process(self, obj):
        if isinstance(obj, list):
            if len(obj) != 2:
                return (None, 'expected list with 2 numbers')
            point = PointGeometry(obj)
            return (point, None)

        if isinstance(obj, dict):
            assert obj.get('type') == SUPPORTED_GEOMETRY_TYPES.POINT
            point = PointGeometry(obj['vertex'])
            return (point, None)

        if not isinstance(obj, PointGeometry):
            return (None, 'expected a PointGeometry object')

        return (obj, None)


class Polygon(Geometry):
    """
    A Schema field representing a Postgis Geometry, represented by a
    GeometryObject. The SqlalchemyStore corresponding field adapter takes care
    of transforming the dict to and from the EWKT strings understood by
    Postgis.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(SUPPORTED_GEOMETRY_TYPES.POLYGON, *args, **kwargs)

    def process(self, obj):
        if isinstance(obj, list):
            polygon = PolygonGeometry(obj)
            return (polygon, None)

        if isinstance(obj, dict):
            geo_type = obj.get('type')
            if geo_type != SUPPORTED_GEOMETRY_TYPES.POLYGON:
                return (None, 'expected a PolygonGeometry object')

            vertices = obj.get('vertices') or []
            if not isinstance(vertices, (tuple, list)):
                return (None, f'unrecognized vertices format: {vertices}')

            polygon = PolygonGeometry(vertices)
            return (polygon, None)

        if not isinstance(obj, PolygonGeometry):
            return (None, 'expected a Polygon object')

        return (obj, None)
