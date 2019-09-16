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
    users = pybiz.Relationship(lambda source: (Account._id, User.account_id), many=True)


if __name__ == '__main__':
    app.bootstrap(namespace=globals())

    def query_users():
        return (
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

    def query_account():
        account = Account(
            _id='deadbeef'*4,
            name='Axial',
            size=69,
        ).save()

        kc = User(
            email='kenneth.chan@axial.net',
            account_id=account._id,
            password='password',
            age=77,
        ).save()

        return (
            Account.select(
                #Account.name,
                #Account.size,
                Account.users.select(
                    #User.email,
                    #User.password,
                    #User.age,
                ).spam(
                    'eggs'
                ).where(
                    User.age > 50
                ).limit(
                    2
                )
            ).where(
                Account.name == 'Axial'
            ).foo(
                'bar'
            ).execute(
                first=True,
                backfill='persistent'
            )
        )


    account = query_account()

    pprint(account)
    pprint(account.users)
    pprint(account.dump())
