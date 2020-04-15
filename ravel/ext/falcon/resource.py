from .constants import HTTP_METHODS, RE_HANDLER_METHOD


class FalconResource(object):
    def __init__(self, route: str = None):
        self._route = route
        self._method_2_endpoint = {}

    def __getattr__(self, key):
        """
        This simulates falcon resource methods, like on_get,
        on_post, etc, when called on the instance.
        """
        match = RE_HANDLER_METHOD.match(key)
        if match is not None:
            method = match.groups()[0].upper()
            if not self.is_method_supported(method):
                error_msg = 'HTTP {} unsupported'.format(method.upper())
                error_msg += ' for URL path {}'.format(self.endpoint)
                raise AttributeError(error_msg)

            endpoint = self.get_endpoint(method)
            return endpoint

        raise AttributeError(key)

    def __repr__(self):
        name_str = get_class_name(self)
        path_str = '(path=' + self.endpoint + ')' if self.endpoint else ''
        return '{}{}'.format(name_str, path_str)

    @property
    def endpoint(self):
        return self._endpoint

    def add_endpoint(self, endpoint):
        # TODO: Raise exception regarding already registered to http method
        self._method_2_endpoint[endpoint.method.upper()] = endpoint

    def get_endpoint(self, method):
        return self._method_2_endpoint.get(method.upper())

    def is_method_supported(self, method):
        is_recognized = method.upper() in HTTP_METHODS
        is_registered = method.upper() in self._method_2_endpoint
        return (is_recognized and is_registered)
