import traceback
import inspect

from typing import Dict
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs

from ravel.util.json_encoder import JsonEncoder
from ravel.util.loggers import console

from .abstract_http_server import AbstractHttpServer, Endpoint


class HttpServer(AbstractHttpServer):
    """
    An HTTP server, using the HTTPServer from the standard library. It sends and
    receives JSON data.
    """

    class Handler(BaseHTTPRequestHandler):
        def process(self):
            try:
                result = self.process_request()
                self.write_headers(status_code=200)
                self.encode_response(result)
            except Exception:
                self.write_headers(status_code=400)
                self.encode_response({
                    'error': 'bad request',
                    'traceback': traceback.format_exc().split('\n')
                })

        def write_headers(self, status_code: int):
            self.send_response(status_code)
            self.send_header('Content-type', 'application/json')
            self.end_headers()

        def process_request(self):
            url = urlparse(self.path)
            http_method = self.command
            body = self.read_body_json_object()
            params = self.extract_query_params(url)

            # Create a merged dict of body and params vars
            # for use in the JsonServer on_request method:
            arguments = body.copy()
            arguments.update(params)

            # Apply action callable
            return self.service.route(
                http_method, url.path, args=(arguments, )
            )

        def encode_response(self, result_data):
            if result_data is not None:
                result = {'data': result_data}
                result_json_str = self.service.json_encode(result)
                self.wfile.write(bytes(result_json_str, 'utf8'))

        def read_body_json_object(self):
            body = {}
            content_len = int(self.headers.get('content-length', 0))
            if content_len:
                body_bytes = self.rfile.read(content_len)
                if body_bytes:
                    body = JsonEncoder.decode(body_bytes.decode())
            return body

        def extract_query_params(self, url) -> Dict:
            params = parse_qs(url.query) if url.query else {}
            for k, v in params.items():
                if len(v) == 1:
                    params[k] = v[0]
            return params

        def do_GET(self):
            self.process()

        def do_PUT(self):
            self.process()

        def do_POST(self):
            self.process()

        def do_PATCH(self):
            self.process()

        def do_DELETE(self):
            self.process()

        def do_OPTIONS(self):
            self.process()

    def __init__(self, json_encode=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.handler = type('Handler', (self.Handler, ), {'service': self})
        self.json_encode = json_encode or JsonEncoder().encode
        self.server = None

    def on_bootstrap(self, host, port):
        super.on_bootstrap(host, port)
        self.server = HTTPServer((self.host, self.port), self.handler)

    def on_start(self):
        console.info(
            f'HTTP server listening on http://{self.host}:{self.port}'
        )
        self.server.serve_forever()

    def on_request(self, action, arguments: Dict) -> dict:
        args, kwargs = [], {}

        for idx, (k, param) in enumerate(action.signature.parameters.items()):
            if not idx:
                continue
            if param.kind == inspect.Parameter.POSITIONAL_OR_KEYWORD:
                if param.default is inspect._empty:
                    args.append(arguments.get(k))
                else:
                    kwargs[k] = arguments.get(k)
            elif param.kind == inspect.Parameter.POSITIONAL_ONLY:
                args.append(arguments.get(k))
            elif param.kind == inspect.Parameter.KEYWORD_ONLY:
                kwargs[k] = arguments.get(k)
        return (args, kwargs)
