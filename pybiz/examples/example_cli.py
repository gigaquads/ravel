import os

from pybiz.api.cli import CommandLineInterface
from pybiz.api.json_server import JsonServer


if os.environ.get('API', 'cli') == 'cli':
    api = CommandLineInterface(name='example_cli', version='1.0.0')
else:
    api = JsonServer(port=8080)

# ----------------------------------------------------------------------
@api(
    http_method='GET',
    url_path='/greet',
    parser={}
)
def speak(name: str, intensity: int=1):
    intensity = int(intensity or 0)
    if intensity > 1:
        msg = '{} must be destroyed!'.format(name.title())
    else:
        msg = 'Hello, {}!'.format(name.title())
    print(msg)
    return msg


@api(
    http_method='GET',
    url_path='/woof',
)
def woof():
    msg = 'Woof Woof!'
    print(msg)
    return msg


# ----------------------------------------------------------------------
if __name__ == '__main__':
    api.start()
