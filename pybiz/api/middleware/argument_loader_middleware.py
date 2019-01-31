import typing

from typing import List, Dict, ForwardRef, Text, Tuple, Set, Type

from pybiz.util import is_bizobj

from .registry_middleware import RegistryMiddleware


class ArgumentLoaderMiddleware(RegistryMiddleware):

    class Loader(object):
        def __init__(self, type_name: Text):
            self.type_name = type_name

        def load(self, proxy, argument, args, kwargs):
            raise NotImplementedError()

        def load_many(self, proxy, argument, args, kwargs):
            raise NotImplementedError()

    class BizObjectLoader(Loader):
        def __init__(
            self,
            target_type: Type['BizObject'],
            source_type: Type = str,  # XXX: deprecated
        ):
            super().__init__(target_type.__name__)
            self.target_type = target_type
            self.source_type = source_type

        def load(self, proxy, _id, args, kwargs):
            return self.target_type.get(_id)

        def load_many(self, proxy, _ids, args, kwargs):
            return self.target_type.get_many(_ids)

    @classmethod
    def from_registry(cls, registry: 'Registry'):
        loaders = [
            cls.BizObjectLoader(bizobj_type)
            for bizobj_type in registry.types.biz.values()
        ]
        return cls(loaders)

    def __init__(self, loaders: List[Loader]):
        super().__init__()
        self.loaders = {}
        for loader in loaders:
            self.loaders[('O', loader.type_name)] = loader.load
            self.loaders[('L', loader.type_name)] = loader.load_many

    def parse_annotation(self, obj):
        key = None
        if isinstance(obj, str):
            key = ('O', obj)
        elif isinstance(obj, type):
            key = ('O', obj.__name__)
        elif isinstance(obj, ForwardRef):
            key = ('O', obj.__forward_arg__)
        elif (
            (isinstance(obj, typing._GenericAlias)) and
            (obj._name in {'List', 'Tuple', 'Set'})
        ):
            if obj.__args__:
                arg = obj.__args__[0]
                key = ('L', self.parse_annotation(arg)[1])
        return key

    def on_request(self, proxy, args, kwargs):
        for idx, (k, param) in enumerate(proxy.signature.parameters.items()):
            if (param.annotation is None):
                continue

            loader_key = self.parse_annotation(param.annotation)
            loader_func = self.loaders.get(loader_key)

            if not loader_func:
                continue

            if idx < len(args) and not is_bizobj(args[idx]):
                val = args[idx]
                args[idx] = loader_func(proxy, val, args, kwargs)
            elif k in kwargs and not is_bizobj(kwargs[k]):
                val = kwargs[k]
                kwargs[k] = loader_func(proxy, val, args, kwargs)
