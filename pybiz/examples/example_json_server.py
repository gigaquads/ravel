from pybiz.api.json_server import JsonServer


api = JsonServer(host='localhost', port=8000)


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
