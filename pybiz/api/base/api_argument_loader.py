from typing import (
    List, Dict, ForwardRef, Text, Tuple, Set, Type,
    _GenericAlias as GenericAlias
)

from pybiz.util.misc_functions import is_bizobj, is_sequence


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

    def __init__(self, api: 'Api'):
        self.biz_types = api.types.biz

    def load(self, proxy: 'Proxy', args: Tuple, kwargs: Dict) -> Tuple:
        """
        Replace args and kwargs with corresponding BizThing and return them
        """
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

    def extract_biz_type_info(self, annotation) -> Tuple[bool, Text]:
        """
        Return a tuple of metadata pertaining to `obj`, which is some object
        used in a type annotation, passed in by the caller.
        """
        key = None
        many = False

        if isinstance(annotation, str):
            key = annotation.split('.')[-1]
        elif isinstance(obj, type):
            key = annotation.__name__.split('.')[-1]
        elif isinstance(annotation, ForwardRef):
            key = annotation.__forward_arg__
        elif (
            (isinstance(annotation, GenericAlias)) and
            (annotation._name in {'List', 'Tuple', 'Set'})
        ):
            if annotation.__args__:
                arg = annotation.__args__[0]
                key = self.extract_biz_type_info(arg)[1]
                many = True

        return (many, key)
