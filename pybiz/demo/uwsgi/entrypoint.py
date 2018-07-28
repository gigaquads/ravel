from demo.api import api


# WSGI callable
uwsgi_entrypoint = api.start

# Process the pybiz manifest file
api.bootstrap()
