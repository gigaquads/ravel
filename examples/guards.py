from pybiz import Application, BizObject, String, Int, Id
from pybiz.app.middleware import Guard, GuardMiddleware


class Exists(Guard):
    def __init__(self, arg: str):
        super().__init__()
        self.arg = arg

    @property
    def description(self):
        return f'{self.arg} exists'

    def execute(self, context, **kwargs):
        biz_obj = kwargs.get(self.arg)
        if not ((biz_obj is not None) and biz_obj.exists()):
            raise self.fail(f'{self.arg} does not exist')


class Matches(Guard):
    def __init__(self, arg: str, **expected: dict):
        super().__init__()
        self.arg = arg
        self.expected = expected

    @property
    def description(self):
        return ', '.join(
            f'{self.arg}.{field_name} == {expected_value}'
            for field_name, expected_value in self.expected.items()
        )

    def execute(self, context, **kwargs):
        biz_object = kwargs.get(self.arg)
        for field_name, expected_value in self.expected.items():
            actual_value = biz_object[field_name]
            if actual_value != expected_value:
                raise self.fail(
                    f'{self.arg}.{field_name} has expected '
                    f'value of ({expected_value}) but got ({actual_value})'
                )


# ----------------------------------------------
if __name__ == '__main__':
    app = Application()

    class Dog(BizObject):
        name = String()
        color = String()
        age = Int()

    @app(guard=Exists('dog') & Matches('dog', color='red'))
    def get_dog(dog: Dog):
        return dog

    app.bootstrap(
        namespace=globals(),
        middleware=[GuardMiddleware()],
    )

    # create a random dog object...
    dog = Dog.generate().save()

    # this should raise an exception regarding the dog having the wrong color
    app.api.get_dog(dog._id)
