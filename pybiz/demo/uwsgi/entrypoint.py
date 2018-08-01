from demo.api import api


uwsgi_callable = api.start
api.bootstrap()  # processes the manifest file
