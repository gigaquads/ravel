import inspect

from pybiz import ApiRegistry


def unpack(signature, request, response) -> dict:
    """
    The `unpack` method transforms the arguments normally dispatched by your web
    framework to its view/api callables into a custom argument list you may have
    in mind. This file demonstrates this.

    The only required argument to the unpack method is a Signature object
    derived from the view callable registered with the api decorator. See the
    docs for the `inspect` module in the standard library to see how you can
    extract useful information from a `Signature`.

    Note that the `unpack` function can also be registered elsewhere instead of
    passed manually as an argument to each api decorator invocation.

    ```python3
    api = ApiRegistry(unpack=unpack)
    ```

    or

    ```python3
    class MyApiRegistry(ApiRegistry):

        def unpack(self, signature, request, **kwargs):
            args_dict = fictitious_get_args_dict(signature, request)
            return args_dict

        def get(self, *args, **kwargs):
            kwargs.setdefault('unpack', self.unpack)
            return super().get(*args, **kwargs)
    ```

    """

    # for instance, get the names of all args and kwargs defined on your view
    # callable, called `func` here.
    arg_names = {
        p.name for p in signature.parameters.values()
        if p.default is inspect._empty
        }

    # now "unpack" the fictitious data contained in the request payload into the
    # arguments to be received by your view callables.
    return {
        k: request['data'].get(k) for k in arg_names
        }


class MyApiRegistry(ApiRegistry):

    def get_request(self, request, response):
        return request

    def get_response(self, request, response):
        return response


# global API registry
api = MyApiRegistry()


@api.post('/login', unpack=unpack)
def login(email, password, age=None):
    print('>>> Logging {} in using password "{}"...'.format(
        email, password))


if __name__ == '__main__':

    # mock request and response objects
    response = None
    request = {
        'data': {
            'email': 'jesus@nazareth.com',
            'password': 'hailtothekingbaby',
            'age': '2017',
            }
        }

    # trigger the "login" view callable with argument unpacking
    api.route('POST', '/login', (request, response))
