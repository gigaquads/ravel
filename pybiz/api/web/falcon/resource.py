from .constants import HTTP_METHODS, RE_HANDLER_METHOD


class Resource(object):
    def __init__(self, url_path:str = None):
        self._url_path = url_path
        self._http_method_2_route = {}

    def __getattr__(self, key):
        """
        This simulates falcon resource methods, like on_get,
        on_post, etc, when called on the instance.
        """
        match = RE_HANDLER_METHOD.match(key)
        if match is not None:
            http_method = match.groups()[0].lower()
            if not self.is_http_method_supported(http_method):
                error_msg = 'HTTP {} unsupported'.format(http_method.upper())
                error_msg += ' for URL path {}'.format(self.url_path)
                raise AttributeError(error_msg)

            route = self.get_route(http_method)
            return route

        raise AttributeError(key)

    def __repr__(self):
        name_str = self.__class__.__name__
        path_str = '(path='+self.url_path+')' if self.url_path else ''
        return '<{}{}>'.format(name_str, path_str)

    @property
    def url_path(self):
        return self._url_path

    def add_route(self, route):
        # TODO: Raise exception regarding already registered to http method
        self._http_method_2_route[route.http_method.lower()] = route

    def get_route(self, http_method):
        return self._http_method_2_route.get(http_method.lower())

    def is_http_method_supported(self, http_method):
        is_recognized = http_method.upper() in HTTP_METHODS
        is_registered = http_method in self._http_method_2_route
        return (is_recognized and is_registered)


class ResourceManager(object):

    def __init__(self):
        self.resources = {}

    def add_route(self, route) -> Resource:
        resource = self.resources.get(route.url_path)
        if resource is None:
            resource = Resource(route.url_path)
            self.resources[route.url_path] = resource
            resource.add_route(route)
            return resource
        else:
            resource.add_route(route)
            return None
