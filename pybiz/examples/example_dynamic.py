import sys

from pybiz.api.repl import Repl
from pybiz.api.json_server import JsonServer


def api_factory():
    api_type = sys.argv[1].lower()
    if api_type == 'repl':
        return Repl()
    elif api_type == 'web':
        return JsonServer(port=8000)
    else:
        raise ValueError('unrecognized api type: {}'.format(api_type))


if __name__ == '__main__':
    api = api_factory()

    @api(http_method='GET', url_path='/greet')
    def greet(name: str, intensity: int = None):
        intensity = int(intensity or 1)
        if intensity == 1:
            message = 'Greetings, {}!'.format(name)
        elif intensity == 2:
            message = 'Fuck you, {}!'.format(name)
        elif intensity >= 3:
            message = ('Fuck you, {}! '.format(name) * 100).rstrip()
        return message

    api.start()
