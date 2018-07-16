import sys

from pybiz.api.repl import Repl, Parser, Arg


api = Repl(name='Repl Test', version='1.0.0')


@api(
    usage='greet a person by their name',
    parser=Parser(
        name='greet',
        usage='greet a person by their name',
        args=[
            Arg(
                flags=('--name', ),
                dtype=str,
                usage="name of person to greet"
            ),
            Arg(
                flags=('--intensity', ),
                dtype=int,
                usage="how angry you are",
                default=1,
            ),
        ]
    )
)
def greet(name, intensity=1):
    if intensity > 1:
        print('{} must be destroyed!'.format(name.title()))
    else:
        print('Hello, {}!'.format(name.title()))


@api()
def speak():
    print('Woof Woof!')


if __name__ == '__main__':
    # use the presence of CLI args to indicate that
    # this REPL should be run as a CLI program
    has_cli_args = len(sys.argv) > 2
    api.start(interactive=(not has_cli_args))
