import pybiz

from appyratus.utils import TimeUtils

startrek = pybiz.Application()


class Officer(pybiz.BizObject):
    first_name = pybiz.String()
    rank = pybiz.String(default='recruit')
    ship_id = pybiz.Field(private=True)
    species = pybiz.Enum(
        pybiz.String(), values=('human', 'klingon', 'vulcan'),
        nullable=False, default='human'
    )
    ship = pybiz.Relationship(
        join=lambda biz_thing: (Officer.ship_id, Ship._id)
    )


class Ship(pybiz.BizObject):
    name = pybiz.String()
    crew = pybiz.Relationship(
        join=lambda biz_thing: (Ship._id, Officer.ship_id),
        order_by=lambda biz_thing: Officer.first_name.asc,
        on_add=lambda self, officer: officer.update({'ship_id': self._id}),
        on_rem=lambda self, officer: officer.update({'ship_id': None}),
        on_del=lambda self, officer: officer.update({'ship_id': None}),
        many=True
    )
    mission_names = pybiz.View(
        load=lambda self: self.missions.name,
    )
    mission_count = pybiz.View(
        load=lambda self: self.missions,
        transform=lambda self, data: len(data),
    )
    mission_count_with_field = pybiz.View(
        load=lambda self: self.missions,
        transform=lambda self, data: str(len(data)),
        field=pybiz.Int(),
    )
    missions = pybiz.Relationship(
        join=lambda biz_thing: (Ship._id, Mission.ship_id),
        many=True
    )


class Mission(pybiz.BizObject):
    name = pybiz.String(nullable=False)
    description = pybiz.String()
    started_at = pybiz.DateTime(default=TimeUtils.utc_now, nullable=False)
    ended_at = pybiz.DateTime()
    ship_id = pybiz.Field()
    status = pybiz.Enum(
        pybiz.String(),
        values=('pending', 'active', 'closed', 'anomalous'),
        default='pending',
        nullable=False,
    )


@startrek()
def get_officer(officer: Officer) -> Officer:
    return officer


@startrek()
def get_ship(ship: Ship = None) -> Ship:
    return ship
