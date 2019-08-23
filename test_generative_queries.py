from pprint import pprint
import pybiz

app = pybiz.Application()


class User(pybiz.BizObject):
    email = pybiz.String()
    age = pybiz.Uint()
    password = pybiz.String()
    account_id = pybiz.UuidString()
    account = pybiz.Relationship(lambda source: (User.account_id, Account._id))


class Account(pybiz.BizObject):
    name = pybiz.String()
    size = pybiz.Int()



if __name__ == '__main__':
    app.bootstrap(namespace=globals())

    users = (
        User.select(
            User.email,
            User.password,
            User.age,
            User.account_id,
            User.account.select(
                Account.name,
                Account.size
            ).where(
                Account.size < 6,
                Account.size > 4
            )
        ).where(
            User.email > 'foo@bar.baz',
            User.age < 50
        ).execute(
            generative=True
        )
    )

    pprint(users.dump())
