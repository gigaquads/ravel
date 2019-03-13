import inspect

from typing import Dict, Text


class RegistryProxy(object):
    def __init__(self, func, decorator: 'RegistryDecorator'):
        super().__init__()
        self.func = func
        self.decorator = decorator
        self.target = self.resolve(func)
        self.signature = inspect.signature(func)
        self.is_async = False

    def __repr__(self):
        return '<{proxy_type}({target_name})>'.format(
            proxy_type=self.__class__.__name__,
            target_name=self.name,
        )

    def __getattr__(self, attr):
        return getattr(self.func, attr, None)

    def __call__(self, *raw_args, **raw_kwargs):
        args, kwargs = self.pre_process(raw_args, raw_kwargs)
        raw_result = self.target(*args, **kwargs)
        result = self.post_process(args, kwargs, raw_result)
        return result

    def pre_process(self, raw_args, raw_kwargs):
        self._apply_middleware_pre_request(raw_args, raw_kwargs)
        args, kwargs = self._apply_registry_on_request(raw_args, raw_kwargs)
        self._apply_middleware_on_request(args, kwargs)
        return (args, kwargs)

    def post_process(self, args, kwargs, raw_result):
        # TODO: call middleware even if exception raised in target, passing on exception
        result = self._apply_registry_on_response(args, kwargs, raw_result)
        self._apply_middleware_post_request(args, kwargs, result)
        return result

    def _apply_middleware_pre_request(self, raw_args, raw_kwargs):
        for m in self.registry.middleware:
            if isinstance(self.registry, m.registry_types):
                m.pre_request(self, raw_args, raw_kwargs)

    def _apply_middleware_on_request(self, prepared_args, prepared_kwargs):
        for m in self.registry.middleware:
            if isinstance(self.registry, m.registry_types):
                m.on_request(self, prepared_args, prepared_kwargs)
        return (prepared_args, prepared_kwargs)

    def _apply_middleware_post_request(
        self, prepared_args, prepared_kwargs, result
    ):
        for m in self.registry.middleware:
            if isinstance(self.registry, m.registry_types):
                m.post_request(self, prepared_args, prepared_kwargs, result)

    def _apply_registry_on_request(self, raw_args, raw_kwargs):
        result = self.registry.on_request(self, *raw_args, **raw_kwargs)
        args, kwargs = result if result else (raw_args, raw_kwargs)
        args, kwargs = self.registry.argument_loader.load(self, args, kwargs)
        return (args, kwargs)

    def _apply_registry_on_response(
        self, prepared_args, prepared_kwargs, result
    ):
        return self.decorator.registry.on_response(
            self, result, *prepared_args, **prepared_kwargs
        )

    @property
    def registry(self) -> 'Registry':
        return self.decorator.registry

    @property
    def name(self) -> Text:
        return self.target.__name__

    @property
    def doc(self):
        return inspect.getdoc(self.target)

    def resolve(self, func):
        return func.target if isinstance(func, RegistryProxy) else func

    def dump(self) -> Dict:
        return {
            'decorator': self.decorator.kwargs,
            'target': self.dump_signature(),
        }

    def dump_signature(self) -> Dict:
        # TODO: move this into a dump component rather than have as method
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
                    if isinstance(param.annotation.str):
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


class AsyncRegistryProxy(RegistryProxy):
    async def __call__(self, *raw_args, **raw_kwargs):
        args, kwargs = self.pre_process(raw_args, raw_kwargs)
        raw_result = await self.target(*args, **kwargs)
        result = self.post_process(args, kwargs, raw_result)
        return result

    def __repr__(self):
        return '<{proxy_type}(async {target_name})>'.format(
            proxy_type=self.__class__.__name__,
            target_name=self.name,
        )
