import inspect

from appyratus.json import JsonEncoder
from appyratus.types import DictAccessor

from pybiz.manifest import Manifest


class Registry(object):
    def __init__(self, manifest=None, middleware=None):
        self.decorators = []
        self.proxies = []
        self._manifest = manifest
        self._is_bootstrapped = False
        self._middleware = middleware or []
        self._json_encoder = JsonEncoder()

    def __call__(self, *args, **kwargs):
        """
        Use this to decorate functions, adding them to this Registry.
        Each time a function is decorated, it arives at the "on_decorate"
        method, where you can registry the function with a web framework or
        whatever system you have in mind.

        Usage:

        ```python3
            api = Registry()

            @api()
            def do_something():
                pass
        ```
        """
        decorator = self.function_decorator_type(self, *args, **kwargs)
        self.decorators.append(decorator)
        return decorator

    @property
    def function_decorator_type(self):
        return RegistryDecorator

    @property
    def function_proxy_type(self):
        return RegistryProxy

    @property
    def manifest(self):
        return self._manifest

    @property
    def middleware(self):
        return self._middleware

    @property
    def biz_types(self) -> DictAccessor:
        return self._manifest.biz_types

    @property
    def dao_types(self) -> DictAccessor:
        return self._manifest.dao_types

    @property
    def schemas(self) -> DictAccessor:
        return self._manifest.schemas

    @property
    def is_bootstrapped(self):
        return self._is_bootstrapped

    def bootstrap(self, manifest_filepath: str=None, defer_processing=False):
        """
        Bootstrap the data, business, and service layers, wiring them up,
        according to the settings contained in a service manifest file.

        Args:
            - filepath: Path to manifest.yml file
        """
        if not self.is_bootstrapped:
            if (self._manifest is None) or (filepath is not None):
                self._manifest = Manifest(manifest_filepath)
            if (self.manifest is not None) and (not defer_processing):
                self._manifest.process()
            self._is_bootstrapped = True

    def dump(self, as_json=False):
        """
        Return a Python dict that can be serialized to JSON, represents the
        contents of the Registry. The purpose of this method is to export
        metadata about this registry to be consumed by some other service or
        external process without said service or process needing to import this
        Registry directly.
        """
        data = {
            'registry': {p.dump() for p in self.proxies}
        }
        return self._json_encoder.encode(data) if as_json else data


    def start(self, *args, **kwargs):
        """
        Enter the main loop in whatever program context your Registry is
        being used, like in a web framework or a REPL.
        """
        raise NotImplementedError('override in subclass')

    def on_decorate(self, proxy: 'RegistryProxy'):
        """
        We come here whenever a function is decorated by this registry. Here we
        can add the decorated function to, say, a web framework as a route.
        """

    def on_request(self, proxy, signature, *args, **kwargs):
        """
        This executes immediately before calling a registered function. You
        must return re-packaged args and kwargs here. However, if nothing is
        returned, the raw args and kwargs are used.
        """
        return (args, kwargs)

    def on_response(self, proxy, result, *args, **kwargs):
        """
        The return value of registered callables come here as `result`. Here
        any global post-processing can be done. Args and kwargs consists of
        whatever raw data was passed into the callable *before* on_request
        executed.
        """
        return result


class RegistryDecorator(object):
    def __init__(self, registry, *args, **params):
        self.registry = registry
        self.params = params

    def __call__(self, func):
        proxy = self.registry.function_proxy_type(func, self)
        self.registry.proxies.append(proxy)
        self.registry.on_decorate(proxy)
        return proxy


class Middleware(object):
    def pre_request(self, args, kwargs):
        pass

    def on_request(self, args, kwargs, prepared_args, prepared_kwargs):
        pass

    def post_request(self, args, kwargs, prepared_args, prepared_kwargs, result):
        pass


class RegistryProxy(object):
    def __init__(self, func, decorator):
        self.func = func
        self.signature = inspect.signature(self.func)
        self.target = self.resolve(func)
        self.decorator = decorator
        self.on_request = decorator.registry.on_request
        self.on_response = decorator.registry.on_response

    def __repr__(self):
        return '<{}({})>'.format(
            self.__class__.__name__,
            ', '.join(['method={}'.format(self.func.__name__)])
        )

    def __call__(self, *raw_args, **raw_kwargs):
        # apply middleware's pre_request methods
        for m in self.registry.middleware:
            m.pre_request(raw_args, raw_kwargs)
        # apply the registry's global on_request method to transform the raw
        # args and kwargs into the format expected by the proxy target callable.
        on_request_retval = self.decorator.registry.on_request(
            self, self.signature, *raw_args, **raw_kwargs
        )
        # apply pre-request middleware
        if on_request_retval:
            prepared_args, prepared_kwargs = on_request_retval
        else:
            prepared_args, prepared_kwargs = raw_args, raw_kwargs
        # apply middleware's on_request methods
        for m in self.registry.middleware:
            m.on_request(raw_args, raw_kwargs, prepared_args, prepared_kwargs)
        result = self.target(*prepared_args, **prepared_kwargs)
        processed_result = self.decorator.registry.on_response(
            self, result, *raw_args, **raw_kwargs
        )
        # apply middleware's post_request methods
        for m in self.registry.middleware:
            m.post_request(
                raw_args, raw_kwargs, prepared_args, prepared_kwargs, result
            )
        # apply post-response middleware
        return processed_result or result

    def __getattr__(self, attr):
        return getattr(self.func, attr)

    @property
    def registry(self):
        return self.decorator.registry

    @property
    def name(self):
        return self.target.__name__

    def resolve(self, func):
        return func.target if isinstance(func, RegistryProxy) else func

    def dump(self):
        return {
            'decorator': self.decorator.params,
            'function': self.dump_signature(),
        }

    def dump_signature(self):
        args, kwargs = [], []
        recognized_param_kinds = {
            Parameter.POSITIONAL_OR_KEYWORD,
            Parameter.POSITIONAL_ONLY,
            Parameter.KEYWORD_ONLY
        }
        for param_name, param in self.signature.parameters.items():
            if param.kind in recognized_param_kinds:
                type_name = None
                if param.annotation != Parameter.empty:
                    if isinstance(param.annotation. str):
                        type_name = param.annotation
                    elif isinstance(param.annotation, type):
                        type_name = param.annotation.__name__
                if k.default == Parameter.empty:
                    args.append({
                        'name': param_name,
                        'type': type_name,
                    })
                else:
                    kwargs.append({
                        'name': param_name,
                        'type': type_name,
                        'default': str(param.default)
                    })

        # get return type name
        returns = ''  # empty string is interpreted to mean "empty"
        if self.signature.return_annotation != Parameter.empty:
            if self.signature.return_annotation is None:
                returns = None
            if isinstance(self.signature.return_annotation, str):
                returns = self.signature.return_annotation
            elif isinstance(self.signature.return_annotation, type):
                returns = self.signature.return_annotation.__name__

        return {
            'name': self.name,
            'args': args,
            'kwargs': kwargs,
            'returns': returns,
        }
