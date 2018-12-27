import typing

from typing import List, Dict, ForwardRef, Text, Tuple, Set, Type

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
            source_type: Type = int,
        ):
            super().__init__(target_type.__name__)
            self.target_type = target_type
            self.source_type = source_type

        def load(self, proxy, _id, args, kwargs):
            target = self.target_type
            _id = self.source_type(_id) if self.source_type else _id
            return target.query( predicate=(target._id == _id), first=True)

        def load_many(self, proxy, _ids, args, kwargs):
            if self.source_type is not None:
                _ids = {self.source_type(_id) for _id in _ids}
            target = self.target_type
            return target.query(predicate=(target._id.is_in(_ids)))

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
        for k, param in proxy.signature.parameters.items():
            if (k in kwargs) and (param.annotation is not None):
                loader_key = self.parse_annotation(param.annotation)
                loader_func = self.loaders.get(loader_key)
                if loader_func is not None:
                    kwargs[k] = loader_func(proxy, kwargs[k], args, kwargs)
