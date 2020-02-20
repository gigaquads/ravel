from appyratus.utils import TimeUtils

import ravel

from ravel import (
    Application, Resource, String, Int, Email, Id,
    relationship
)


facebook = ravel.Application()


class User(Resource):
    email = Email(required=True)
    created_at = DateTime(required=True, default=True)

    @relationship(
        join=lambda: [
            (User._id, UserFriend.owner_user_id),
            (UserFriend.befriended_user_id, User._id)
        ],
        many=True
    )
    def friends(self, request):
        return request.result


class UserFriend(Resource):
    owner_user_id = Id(lambda: User, required=True)
    befriended_user_id = Id(lambda: User, required=True)





'''
class Officer(ravel.Resource):
    first_name = ravel.String()
    rank = ravel.String(default='recruit')
    ship_id = ravel.Field(private=True)
    species = ravel.Enum(
        ravel.String(),
        values=('human', 'klingon', 'vulcan'),
        nullable=False,
        default='human'
    )
    ship = ravel.Relationship(join=lambda entity: (Officer.ship_id, Ship._id))


class Ship(ravel.Resource):
    name = ravel.String()
    crew = ravel.Relationship(
        join=lambda entity: (Ship._id, Officer.ship_id),
        order_by=lambda entity: Officer.first_name.asc,
        on_add=lambda self, officer: officer.update({'ship_id': self._id}),
        on_rem=lambda self, officer: officer.update({'ship_id': None}),
        on_del=lambda self, officer: officer.update({'ship_id': None}),
        many=True
    )
    mission_names = ravel.View(load=lambda self: self.missions.name, )
    mission_count = ravel.View(
        load=lambda self: self.missions,
        transform=lambda self, data: len(data),
    )
    mission_count_with_field = ravel.View(
        load=lambda self: self.missions,
        transform=lambda self, data: str(len(data)),
        field=ravel.Int(),
    )
    missions = ravel.Relationship(
        join=lambda entity: (Ship._id, Mission.ship_id), many=True
    )


class Mission(ravel.Resource):
    name = ravel.String(nullable=False)
    description = ravel.String()
    started_at = ravel.DateTime(default=TimeUtils.utc_now, nullable=False)
    ended_at = ravel.DateTime()
    ship_id = ravel.Field()
    status = ravel.Enum(
        ravel.String(),
        values=('pending', 'active', 'closed', 'anomalous'),
        default='pending',
        nullable=False,
    )


class Planet(ravel.Resource):
    resources = fields.Nested(
        {
            'water': fields.Bool(),
            'wine': fields.List(fields.String())
        }
    )


@startrek()
def get_officer(officer: Officer) -> Officer:
    return officer


@startrek()
def get_ship(ship: Ship = None) -> Ship:
    return ship
'''
