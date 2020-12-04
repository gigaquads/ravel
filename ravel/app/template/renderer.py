from typing import Dict, Text


class TemplateRenderer:
    def render(self, template: Text, context: Dict = None) -> Text:
        """
        Render a template to string, interpolating with context.

        Args:
            - template: filename of the template, like example.html
            - context: the template context data dict
        """
        raise NotImplementedError()