from pybiz import ApiRegistry


def web_framework_hook(handler):
    """
    Fictitious function, which does whatever is necessary to register a request
    handler/view function with your web framework, e.g. Django, Falcon, etc.

    Note that in this example, we manually set this function as the `hook`
    keyword argument each time with use the `app.post` decorator. There are two
    alternative ways of setting this hook only once.

    The First option is to derive an `ApiRegistry` subclass along with its post,
    get, patch, put, etc. methods, such that the `hook` kwarg is always set to
    the desired function by default. Like so:

    ```python
    class MyApiRegistry(ApiRegistry):

        def hook(self, handler):
            print('Registering handler with web framework!')

        def get(self, *args, **kwargs):
            kwargs.setdefault('hook', self.hook)
            return super().get(*args, **kwargs)
    ```

    The second is to pass in the function as the `hook` constructor kwarg:

    ```python
    api = ApiRegistry(hook=web_framework_hook)
    ```

    """
    print(
        'Registering {} {} route with your '
        'web framework using function "{}".'.format(
            handler.http_method.upper(),
            handler.path,
            handler.target.__name__,
        ))


class MyApiRegistry(ApiRegistry):

    def get_request(self, request, response):
        return request

    def get_response(self, request, response):
        return response


# global API registry
api = MyApiRegistry()


# Now, register some callables with the registry:

@api.post('/login', hook=web_framework_hook)
def login(request, response):
    print('>>> Logging in...')


@api.post('/logout', hook=web_framework_hook)
def logout(request, response):
    print('>>> Logging out...')


if __name__ == '__main__':

    # mock request and response objects
    request = response = None

    api.route('POST', '/login', (request, response))
    api.route('POST', '/logout', (request, response))
