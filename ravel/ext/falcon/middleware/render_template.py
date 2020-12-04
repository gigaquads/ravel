from typing import Callable, Tuple, Dict

from ravel import Middleware
from ravel.app.template import JinjaTemplateRenderer


class RenderTemplate(Middleware):
    def __init__(self, renderer: 'TemplateRenderer', mime_type='text/html'):
        self._renderer = renderer
        self._content_type = mime_type

    def post_request(
        self,
        action: 'Action',
        request: 'Request',
        result,
    ):
        if action.template is not None:
            assert isinstance(action.template, str)
            assert (result is None) or isinstance(result, dict)
            tpl_filename = action.template
            http_resp = request.raw_args[1]
            http_resp.content_type = self._content_type 
            http_resp.body = self._renderer.render(tpl_filename, result)