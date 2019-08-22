from pprint import pprint
import pybiz

app = pybiz.Application()


class User(pybiz.BizObject):
    email = pybiz.String()
    password = pybiz.String()
    account_id = pybiz.Field()
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
            User.account.select(
                Account.name,
                Account.size
            )
        ).execute(
            generative=True
        )
    )

    pprint(users.dump())
