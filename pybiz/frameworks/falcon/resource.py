from pybiz.const import RE_HANDLER_METHOD, HTTP_METHODS


class FalconResourceManager(object):

    def build_resource_classes(self, api_registry):
        resource_class_list = []
        for url_path, http_method_handlers in api_registry.handlers.items():
            resource_class = self.new_resource_class(url_path)
            resource_class_list.append(resource_class)
            for http_method, handler in http_method_handlers.items():
                resource_class.add_handler(http_method, handler)
        return resource_class_list

    def new_resource_class(self, default_url_path=None):
        class ApiResource(object):

            _default_url_path = default_url_path
            _handlers = {}

            @classmethod
            def get_default_url_path(cls):
                return cls._default_url_path

            @classmethod
            def add_handler(cls, http_method, handler):
                http_method = http_method.lower()
                assert http_method not in cls._handlers
                cls._handlers[http_method] = handler

            @classmethod
            def get_handler(cls, http_method=None, request=None):
                assert http_method or request
                if http_method is not None:
                    return cls._handlers.get(http_method.lower())
                elif request is not None:
                    return cls._handlers.get(request.method.lower())

            @classmethod
            def is_http_method_supported(cls, http_method):
                is_recognized = http_method.upper() in HTTP_METHODS
                is_registered = http_method in cls._handlers
                return (is_recognized and is_registered)

            def __init__(self, url_path:str = None):
                self._url_path = url_path or self.get_default_url_path()
                assert self._url_path

            def __repr__(self):
                default_path = self.url_path
                path_str = '(path='+default_path+')' if default_path else ''
                name_str = self.__class__.__name__
                return '<{}{}>'.format(name_str, path_str)

            def __getattr__(self, key):
                """
                This simulates falcon resource methods, like on_get,
                on_post, etc, when accessed on the instance.
                """
                match = RE_HANDLER_METHOD.match(key)
                if match is not None:
                    http_method = match.groups()[0].lower()

                    if not self.is_http_method_supported(http_method):
                        error_msg = 'HTTP {} not supported'.format(
                                http_method.upper())

                        if self.url_path:
                            path = self.url_path
                            error_msg += ' for URL path {}'.format(path)

                        raise AttributeError(error_msg)

                    handler = self.get_handler(http_method=http_method)
                    return handler

                raise AttributeError(key)

            @property
            def url_path(self):
                return self._url_path

        # return the new dynamic class defined above
        return ApiResource


if __name__ == '__main__':
    from pybiz import BizObject
    from pybiz.api import ApiRegistry

    api = ApiRegistry()

    class User(BizObject):
        @classmethod
        def schema(cls):
            return None

        @api.get('/users/{user_id}')
        def get_user(request, response, user_id):
            return {'id': user_id}

    builder = FalconResourceManager()
    resource_classes = builder.build_resource_classes(api)
    ApiResource = resource_classes[0]
    res = ApiResource()
