import pybiz


class Person(pybiz.BizObject):
    first_name = pybiz.String()
    rank = pybiz.String(default='recruit')
    ship_id = pybiz.Field(private=True)
    ship = pybiz.Relationship(lambda self: (Person.ship_id, Ship._id))


class Ship(pybiz.BizObject):
    name = pybiz.String()
    crew = pybiz.Relationship(lambda self: (Ship._id, Person.ship_id), many=True)
