import typing

from typing import List, Dict, ForwardRef, Text, Tuple, Set, Type

from pybiz.util import is_bizobj

from .base import RegistryMiddleware

LOAD_ONE  = 1
LOAD_MANY = 2


class ArgumentLoaderMiddleware(RegistryMiddleware):

    class Loader(object):
        def __init__(self, type_name: Text):
            self.type_name = type_name

        def load(self, proxy, argument, args, kwargs):
            raise NotImplementedError()

        def load_many(self, proxy, argument, args, kwargs):
            raise NotImplementedError()

    class BizObjectLoader(Loader):
        def __init__(self, biz_type: Type['BizObject']):
            super().__init__(biz_type.__name__)
            self.biz_type = biz_type

        def load(self, proxy, _id, args, kwargs):
            return self.biz_type.get(_id)

        def load_many(self, proxy, _ids, args, kwargs):
            return self.biz_type.get_many(_ids)

    def __init__(self):
        super().__init__()
        self._load_funcs = {}

    def on_bootstrap(self):
        for biz_type in self.registry.types.biz.values():
            self._load_funcs[LOAD_ONE, loader.type_name] = loader.load
            self._load_funcs[LOAD_MANY, loader.type_name] = loader.load_many

    def on_request(self, proxy, args, kwargs):
        """
        For any positional or keyword argument declared on a proxy target
        function, try to replace it if the actual bound value is not a BizObject
        type, assuming that this non-BizObject value is an ID or sequence of
        IDs.
        """
        new_args = list(args)

        for idx, (k, param) in enumerate(proxy.signature.parameters.items()):
            if (param.annotation is None):
                continue

            # get loader function
            load = self._get_loader_func_for_param(param)

            if idx < len(new_args) and not is_bizobj(new_args[idx]):
                # bind to a positional argument
                val = new_args[idx]
                new_args[idx] = load(proxy, val, args, kwargs)
            elif k in kwargs and not is_bizobj(kwargs[k]):
                # bind to a keyword argument
                val = kwargs[k]
                kwargs[k] = load(proxy, val, args, kwargs)

            return (tuple(new_args), kwargs)

    def _get_loader_func_for_param(self, param):
        key = self._get_loader_func(param.annotation)
        return self._load_funcs.get(key)

    def _parse_type_annotation(self, obj):
        key = None

        if isinstance(obj, str):
            key = (LOAD_ONE, obj)
        elif isinstance(obj, type):
            key = (LOAD_ONE, obj.__name__)
        elif isinstance(obj, ForwardRef):
            key = (LOAD_ONE, obj.__forward_arg__)
        elif (
            (isinstance(obj, typing._GenericAlias)) and
            (obj._name in {'List', 'Tuple', 'Set'})
        ):
            if obj.__args__:
                arg = obj.__args__[0]
                key = (LOAD_MANY, self._parse_type_annotation(arg)[1])

        return key
