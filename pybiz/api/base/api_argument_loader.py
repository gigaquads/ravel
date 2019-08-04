from inspect import Parameter
from collections import defaultdict
from typing import (
    List, Dict, ForwardRef, Text, Tuple, Set, Type,
    _GenericAlias as GenericAlias
)

from pybiz.util.misc_functions import (
    is_bizobj, is_sequence, extract_biz_info_from_annotation
)


class ApiArgumentLoader(object):
    """
    A `ApiArgumentLoader` instance is used by each `Api` object. When enabled
    (which it is, by default), arguments passed into API endpoints (AKA ApiProxy
    objects) are automatically loaded or converted to their corresponding
    BizObjects, replacing the raw arguments, provided that the arguments are
    declared using a BizObject as the type annotation.

    For example, if you have an API endpoint, like:

    ```python3
    @repl()
    def get_projects(user: User) -> Project.BizList:
        return user.projects
    ```

    Then the `ApiArgumentLoader` makes it possible to call this method in the
    following ways:

    ```python3
    get_projects(user)
    get_projects("user-id-123")
    get_projects({"id": "user-id-124", "name": "Musk"})
    ```

    In the first case, a `User` BizObject is passed in. In the second, a User ID
    is passed in, and the argument loader performs `User.get(_id)`. Finally, in
    the third case, a raw dict is passed in with the format specified by the
    `User` BizObject schema and is converted into the corresponding BizObject.
    """

    class ArgumentSpec(object):
        def __init__(
            self, position: int, arg_name: Text,
            many: bool, biz_type: Type['BizObject']
        ):
            self.position = position
            self.arg_name = arg_name
            self.many = many
            self.biz_type = biz_type

    def __init__(self, api: 'Api'):
        self._biz_types = api.types.biz
        self._proxy_2_specs = defaultdict(list)
        for proxy in api.proxies.values():
            for idx, param in enumerate(proxy.signature.parameters.values()):
                ann = param.annotation
                many, biz_type_name = extract_biz_info_from_annotation(ann)
                biz_type = self._biz_types.get(biz_type_name)
                if biz_type is not None:
                    position = (
                        idx if param.default == Parameter.empty else None
                    )
                    spec = ApiArgumentLoader.ArgumentSpec(
                        idx, param.name, many, biz_type
                    )
                    self._proxy_2_specs[proxy].append(spec)

    def load(self, proxy: 'ApiProxy', args: Tuple, kwargs: Dict) -> Tuple:
        """
        Replace args and kwargs with corresponding BizThing and return them
        """
        loaded_args = list(args)
        loaded_kwargs = kwargs.copy()

        for spec in self._proxy_2_specs[proxy]:
            if spec.position is not None:
                unloaded = args[spec.position]
                partition = loaded_args
                key = spec.position
            else:
                unloaded = kwargs.get(spec.arg_name)
                partition = loaded_kwargs
                key = spec.arg_name

            partition[key] = self.load_param(
                spec.many, spec.biz_type, unloaded
            )

        return (loaded_args, loaded_kwargs)

    def load_param(self, many: bool, biz_type: Type['BizObject'], preloaded):
        """
        Convert the given parameter "preloaded" into its corresponding
        BizThing.

        - If the preloaded value is an ID, fetch the object.
        - If it is a list of IDs, return a BizList.
        - If it is a dict, replace it with a corresponding BizObject instance.
        """
        if not (preloaded and biz_type):
            return preloaded
        elif not many:
            if is_bizobj(preloaded):
                return preloaded
            elif isinstance(preloaded, dict):
                if 'id' in preloaded:
                    preloaded['_id'] = preloaded.pop('id')
                if 'rev' in preloaded:
                    preloaded['_rev'] = preloaded.pop('rev')
                return biz_type(preloaded)
            else:
                return biz_type.get(_id=preloaded)
        elif is_sequence(preloaded):
            if isinstance(preloaded, set):
                preloaded = list(preloaded)
            elif is_bizobj(preloaded[0]):
                return biz_type.BizList(preloaded)
            elif isinstance(preloaded[0], dict):
                return biz_type.BizList(
                    biz_type(data) for data in preloaded
                )
            else:
                return biz_type.get_many(_ids=preloaded)
