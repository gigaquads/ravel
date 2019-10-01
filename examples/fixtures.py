import pybiz


class User(pybiz.BizObject):
    first_name = pybiz.String()
    last_name = pybiz.String()
    email = pybiz.Email()
    phone = pybiz.String()
    average_rating = pybiz.Float()
    cars = pybiz.Relationship(lambda user: (User._id, Car.owner_user_id), many=True)
    reviews_sent = pybiz.Relationship(lambda user: (User._id, Review.reviewer_user_id), many=True)
    reviews_received = pybiz.Relationship(lambda user: (User._id, Review.reviewee_user_id), many=True)
    trips_driven = pybiz.Relationship(lambda user: (User._id, Trip.driver_user_id), many=True)
    trips_ridden = pybiz.Relationship(
        join=(
            lambda user: (User._id, TripRider.user_id),
            lambda trip_rider: (TripRider.trip_id, Trip._id),
        ),
        many=True
    )

class Car(pybiz.BizObject):
    make = pybiz.String()
    model = pybiz.String()
    year = pybiz.String()
    color = pybiz.String()
    capacity = pybiz.Uint()
    owner_user_id = pybiz.Id()
    owner = pybiz.Relationship(lambda car: (Car.owner_user_id, User._id))


class Review(pybiz.BizObject):
    rating = pybiz.Float()
    comment = pybiz.String()
    trip_id = pybiz.Id()
    reviewer_user_id = pybiz.Id()
    reviewee_user_id = pybiz.Id()
    trip = pybiz.Relationship(lambda review: (Review.trip_id, Trip._id))
    reviewer = pybiz.Relationship(lambda review: (Review.reviewer_user_id, User._id))
    reviewee = pybiz.Relationship(lambda review: (Review.reviewee_user_id, User._id))


class Trip(pybiz.BizObject):
    pick_up_location_id = pybiz.Id()
    drop_off_location_id = pybiz.Id()
    driver_user_id = pybiz.Id()
    pick_up = pybiz.Relationship(lambda trip: (Trip.pick_up_location_id, Location._id))
    drop_off = pybiz.Relationship(lambda trip: (Trip.drop_off_location_id, Location._id))
    driver = pybiz.Relationship(lambda trip: (Trip.driver_user_id, User._id))
    riders = pybiz.Relationship((
        lambda trip: (Trip.driver_user_id, TripRider.trip_id),
        lambda rider: (TripRider.user_id, User._id),
    ))


class TripRider(pybiz.BizObject):
    trip_id = pybiz.Id()
    user_id = pybiz.Id()
    

class Location(pybiz.BizObject):
    name = pybiz.String()
    address = pybiz.String()
    postal_code = pybiz.String()
    city = pybiz.String()
    state = pybiz.String()


if __name__ == '__main__':
    from pprint import pprint

    app = pybiz.Application().bootstrap(namespace=globals())

    user = User.select(
        User.first_name,
        User.email,
        User.phone,
        User.trips_ridden.select(
            Trip.drop_off,
            Trip.pick_up.select().where(
                Location.state == 'New York',
                Location.city == 'Brooklyn',
            ),
            Trip.driver.select(
                User.first_name,
                User.reviews_received.select(
                    Review.rating,
                    Review.comment,
                )
                .where(
                    Review.rating > 3.5
                )
                .limit(2)
            )
            .where(
                User.average_rating >= 4.0
            )
        )
        .limit(2)
    ).execute(
        first=True,
        backfill='ephemeral'
    )

    pprint(user.dump())
