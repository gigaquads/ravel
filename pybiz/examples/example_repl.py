from pybiz.api.repl import Repl


api = Repl()


@api()
def speak():
    print('woof')


@api()
def greet(name):
    print('hello, {}'.format(name))


if __name__ == '__main__':
    api.start()
