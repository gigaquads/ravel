import inspect

from typing import Dict, Text

from appyratus.json import JsonEncoder
from appyratus.utils import DictAccessor

from pybiz.manifest import Manifest

from .registry_object import RegistryObject



class RegistryProxy(RegistryObject):
    def __init__(self, func, decorator: 'RegistryDecorator'):
        super(). __init__()
        self.func = func
        self.decorator = decorator
        self.target = self.resolve(func)
        self.signature = inspect.signature(func)

    def __repr__(self):
        return '<{proxy_type}({target_name})>'.format(
            proxy_type=self.__class__.__name__,
            target_name=self.name,
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
    def registry(self) -> 'Registry':
        return self.decorator.registry

    @property
    def name(self) -> Text:
        return self.target.__name__

    def resolve(self, func):
        return func.target if isinstance(func, RegistryProxy) else func

    def dump(self) -> Dict:
        return {
            'decorator': self.decorator.kwargs,
            'target': self.dump_signature(),
        }

    def dump_signature(self) -> Dict:
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
