from inspect import Parameter
from collections import defaultdict
from typing import (
    List, Dict, ForwardRef, Text, Tuple, Set, Type, Callable,
    _GenericAlias as GenericAlias
)

from pybiz.constants import ID_FIELD_NAME, REV_FIELD_NAME
from pybiz.util.misc_functions import (
    is_biz_obj, is_sequence, extract_biz_info_from_annotation
)


class ApplicationArgumentLoader(object):
    """
    A `ApplicationArgumentLoader` instance is used by each `Application` object. When enabled
    (which it is, by default), arguments passed into API endpoints (AKA Endpoint
    objects) are automatically loaded or converted to their corresponding
    BizObjects, replacing the raw arguments, provided that the arguments are
    declared using a BizObject as the type annotation.

    For example, if you have an API endpoint, like:

    ```python3
    @repl()
    def get_projects(user: User) -> Project.BizList:
        return user.projects
    ```

    Then the `ApplicationArgumentLoader` makes it possible to call this method in the
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
            many: bool, biz_class: Type['BizObject']
        ):
            self.position = position
            self.arg_name = arg_name
            self.many = many
            self.biz_class = biz_class

    def __init__(self, app: 'Application', on_load: Callable = None):
        self._on_load = (
            on_load or (lambda spec, raw_value, loaded_value: loaded_value)
        )
        self._biz_classes = app.types.biz
        self._endpoint_2_specs = defaultdict(list)
        for endpoint in app.endpoints.values():
            for idx, param in enumerate(endpoint.signature.parameters.values()):
                ann = param.annotation
                many, biz_class_name = extract_biz_info_from_annotation(ann)
                biz_class = self._biz_classes.get(biz_class_name)
                if biz_class is not None:
                    position = (
                        idx if param.default == Parameter.empty else None
                    )
                    spec = ApplicationArgumentLoader.ArgumentSpec(
                        idx, param.name, many, biz_class
                    )
                    self._endpoint_2_specs[endpoint].append(spec)

    def load(self, endpoint: 'Endpoint', args: Tuple, kwargs: Dict) -> Tuple:
        """
        Replace args and kwargs with corresponding BizThing and return them
        """
        loaded_args = list(args)
        loaded_kwargs = kwargs.copy()

        for spec in self._endpoint_2_specs[endpoint]:
            if spec.position is not None and spec.position < len(args):
                raw_arg_value = args[spec.position]
                loaded_args_or_kwargs = loaded_args
                key = spec.position
            else:
                raw_arg_value = kwargs.get(spec.arg_name)
                loaded_args_or_kwargs = loaded_kwargs
                key = spec.arg_name

            # Note that "key" is either a position integer offset
            # of the name of a keyword argument.

            loaded_biz_thing = self.load_param(
                spec.many, spec.biz_class, raw_arg_value
            )
            # store a reference to the raw argument value on the loaded BizThing
            # so that it can still be accessed inside the app.
            if loaded_biz_thing is not None:
                loaded_biz_thing.internal.arg = raw_arg_value

            loaded_args_or_kwargs[key] = self._on_load(
                spec, raw_arg_value, loaded_biz_thing
            )
        return (loaded_args, loaded_kwargs)

    def load_param(self, many: bool, biz_class: Type['BizObject'], preloaded):
        """
        Convert the given parameter "preloaded" into its corresponding
        BizThing.

        - If the preloaded value is an ID, fetch the object.
        - If it is a list of IDs, return a BizList.
        - If it is a dict, replace it with a corresponding BizObject instance.
        """
        if not (preloaded and biz_class):
            return preloaded
        elif not many:
            if is_biz_obj(preloaded):
                return preloaded
            elif isinstance(preloaded, dict):
                if 'id' in preloaded:
                    preloaded[ID_FIELD_NAME] = preloaded.pop('id')
                if 'rev' in preloaded:
                    preloaded[REV_FIELD_NAME] = preloaded.pop('rev')
                return biz_class(preloaded)
            else:
                return biz_class.get(_id=preloaded)
        elif is_sequence(preloaded):
            if isinstance(preloaded, set):
                preloaded = list(preloaded)
            if is_biz_obj(preloaded[0]):
                return biz_class.BizList(preloaded)
            elif isinstance(preloaded[0], dict):
                return biz_class.BizList(
                    biz_class(record).clean() if record.get('id') is not None
                    else biz_class(record)
                        for record in preloaded
                )
            else:
                return biz_class.get_many(_ids=preloaded)
