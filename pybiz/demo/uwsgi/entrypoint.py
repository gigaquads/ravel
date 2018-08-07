from demo.api import http


uwsgi_callable = http.start
http.bootstrap()  # processes the manifest file
