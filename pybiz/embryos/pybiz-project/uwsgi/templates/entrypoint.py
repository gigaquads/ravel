from os.path import dirname, realpath, join

from auth.api import web

manifest_filepath = join(dirname(dirname(realpath(__file__))), 'manifest.yml')
uwsgi_callable = web.start

web.bootstrap(manifest_filepath)
