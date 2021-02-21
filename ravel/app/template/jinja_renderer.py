from typing import Text, List, Dict
from appyratus.utils.template_utils import JinjaTemplateEnvironment

from .renderer import TemplateRenderer


class JinjaTemplateRenderer(TemplateRenderer):

    def __init__(self, path: List[Text], **jinja_env_kwargs):
        search_path = path if isinstance(path, str) else [path]
        self._jinja_env = JinjaTemplateEnvironment(search_path=path, **jinja_env_kwargs)

    def render(self, template: Text, context: Dict = None) -> Text:
        template_obj = self._jinja_env.from_filename(template)
        return template_obj.render(context or {})
