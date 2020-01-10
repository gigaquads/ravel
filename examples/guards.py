from uuid import uuid4

from pybiz import BizObject, String, Int
from pybiz.app import Application
from pybiz.app.middleware import GuardMiddleware, Guard

app = Application()


class Dog(BizObject):
    name = String()
    color = String()
    age = Int()


class Exists(Guard):
    def __init__(self, *names, **kwargs):
        super().__init__(**kwargs)
        self.names = list(names)

    @property
    def description(self):
        s = 's' if len(self.names) == 1 else ''
        return f'{", ".join(self.names)} exist{s}'

    def execute(self, context, **kwargs):
        for name in self.names:
            biz_obj = kwargs.get(name)
            if not ((biz_obj is None) and biz_obj.exists()):
                raise self.fail(f'{name} does not exist')


@app(guard=Exists('dog'))
def get_dog(dog: Dog):
    return dog


if __name__ == '__main__':
    app.bootstrap(
        namespace=globals(),
        middleware=[GuardMiddleware()]
    ).start()

    dog_id = uuid4().hex
    dog = app.api.get_dog(dog_id)
