import pybiz

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
        join=lambda self: (Officer.ship_id, Ship._id)
    )


class Ship(pybiz.BizObject):
    name = pybiz.String()
    crew = pybiz.Relationship(
        join=lambda self: (Ship._id, Officer.ship_id),
        order_by=lambda self: Officer.first_name.asc,
        on_add=lambda self, officer: officer.update({'ship_id': self._id}),
        on_rem=lambda self, officer: officer.update({'ship_id': None}),
        on_del=lambda self, officer: officer.update({'ship_id': None}),
        many=True
    )


@startrek()
def get_officer(officer: 'Officer') -> 'Person':
    return officer
