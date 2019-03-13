from typing import (
    List, Dict, ForwardRef, Text, Tuple, Set, Type,
    _GenericAlias as GenericAlias
)

from pybiz.util import is_bizobj


class RegistryArgumentLoader(object):
    def __init__(self, registry: 'Registry'):
        self.biz_types = registry.types.biz

    def load(
        self,
        proxy: 'RegistryProxy',
        args: Tuple,
        kwargs: Dict
    ) -> Tuple[Tuple, Dict]:
        loaded_args = []
        loaded_kwargs = {}
        for idx, param in enumerate(proxy.signature.parameters.values()):
            many, biz_type_name = self.extract_biz_type_info(param.annotation)
            biz_type = self.biz_types.get(biz_type_name)
            if idx < len(args):
                arg = args[idx]
                loaded_arg = self.load_param(many, biz_type, arg)
                loaded_args.append(loaded_arg)
            else:
                kwarg = kwargs.get(param.name)
                loaded_kwarg = self.load_param(many, biz_type, kwarg)
                loaded_kwargs[param.name] = loaded_kwarg
        return (loaded_args, loaded_kwargs)

    def load_param(
        self,
        many: bool,
        biz_type: Type['BizObject'],
        preloaded_value
    ):
        if not (preloaded_value and biz_type):
            return preloaded_value
        elif not many:
            if is_bizobj(preloaded_value):
                return preloaded_value
            elif isinstance(preloaded_value, dict):
                return biz_type(preloaded_value)
            else:
                return biz_type.get(_id=preloaded_value)
        elif isinstance(preloaded_value, (list, tuple, set)):
            if isinstance(preloaded_value, set):
                preloaded_value = list(preloaded_value)
            elif is_bizobj(preloaded_value[0]):
                return biz_type.BizList(preloaded_value)
            elif isinstance(preloaded_value[0], dict):
                return biz_type.BizList(
                    biz_type(data) for data in preloaded_value
                )
            else:
                return biz_type.get_many(_ids=preloaded_value)

    def extract_biz_type_info(self, obj) -> Tuple[bool, Text]:
        key = None
        many = False
        if isinstance(obj, str):
            key = obj.split('.')[-1]
        elif isinstance(obj, type):
            key = obj.__name__.split('.')[-1]
        elif isinstance(obj, ForwardRef):
            key = obj.__forward_arg__
        elif (
            (isinstance(obj, GenericAlias)) and
            (obj._name in {'List', 'Tuple', 'Set'})
        ):
            if obj.__args__:
                arg = obj.__args__[0]
                key = self.extract_biz_type_info(arg)[1]
                many = True
        return (many, key)
