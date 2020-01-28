from pybiz import Application, BizObject, String, Int, Id
from pybiz.app.middleware import Guard, GuardMiddleware


class Exists(Guard):
    """
    This guard ensures that a named endpoint argument exists in the DAL.
    """

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
    """
    This guard ensures that a named endpoint argument has one or more attributes
    equal to the given values, specified through __init__ kwargs.
    """

    def __init__(self, arg: str, **expected: dict):
        super().__init__()
        self.arg = arg
        self.expected = expected

    @property
    def description(self):
        return ', '.join(
            f'{self.arg}.{field_name} is {expected_value}'
            for field_name, expected_value in self.expected.items()
        )

    def execute(self, context, **kwargs):
        biz_object = kwargs.get(self.arg)
        for field_name, expected_value in self.expected.items():
            actual_value = biz_object[field_name]
            if actual_value != expected_value:
                raise self.fail(
                    f'{self.arg}.{field_name} has expected '
                    f'value of {expected_value} but got {actual_value}'
                )


# program point of entry:
if __name__ == '__main__':
    app = Application()

    # define a "dog" buisness object
    class Dog(BizObject):
        name = String()
        color = String()
        age = Int()

    # define an endpoint with a guard
    @app(guard=Exists('dog') & Matches('dog', color='red'))
    def get_dog(dog: Dog):
        return dog

    # initialize the application with GuardMiddleware
    app.bootstrap(
        namespace=globals(),
        middleware=[GuardMiddleware()],
    )

    # create a dog object
    dog = Dog(name='Frank', color='brown', age=8).save()

    # raise exception because dog has the wrong color (not red)
    app.api.get_dog(dog._id)
