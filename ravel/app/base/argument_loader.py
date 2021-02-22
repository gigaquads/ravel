from inspect import Parameter
from collections import defaultdict
from typing import (
    List, Dict, ForwardRef, Text, Tuple, Set, Type, Callable,
    _GenericAlias as GenericAlias
)

from ravel.constants import ID, REV
from ravel.util.misc_functions import (
    extract_res_info_from_annotation,
    is_sequence,
)

from ravel.util import (
    is_resource
)

class ArgumentLoader(object):
    """
    A `ArgumentLoader` instance is used by each `Application` object. When enabled
    (which it is, by default), arguments passed into API actions (AKA Action
    objects) are automatically loaded or converted to their corresponding
    Resources, replacing the raw arguments, provided that the arguments are
    declared using a Resource as the type annotation.

    For example, if you have an API action, like:

    ```python3
    @repl()
    def get_projects(user: User) -> Project.Batch:
        return user.projects
    ```

    Then the `ArgumentLoader` makes it possible to call this method in the
    following ways:

    ```python3
    get_projects(user)
    get_projects("user-id-123")
    get_projects({"id": "user-id-124", "name": "Musk"})
    ```

    In the first case, a `User` Resource is passed in. In the second, a User ID
    is passed in, and the argument loader performs `User.get(_id)`. Finally, in
    the third case, a raw dict is passed in with the format specified by the
    `User` Resource schema and is converted into the corresponding Resource.
    """

    class ArgumentSpec(object):
        def __init__(
            self,
            position: int,
            arg_name: Text,
            many: bool,
            resource_type: Type['Resource']
        ):
            self.position = position
            self.arg_name = arg_name
            self.many = many
            self.resource_type = resource_type

    def __init__(self, app: 'Application', on_load: Callable = None):
        self._app = app
        self._on_load = (
            on_load or (lambda spec, raw_value, loaded_value: loaded_value)
        )
        self._action_2_specs = defaultdict(list)
        self.bind()

    def bind(self):
        self._action_2_specs = defaultdict(list)
        for action in self._app.actions.values():
            self._action_2_specs[action] = []
            for idx, param in enumerate(action.signature.parameters.values()):
                ann = param.annotation
                many, res_class_name = extract_res_info_from_annotation(ann)
                if res_class_name is None:
                    continue
                resource_type = self._app[res_class_name]
                if resource_type is not None:
                    position = (
                        idx if param.default == Parameter.empty else None
                    )
                    spec = ArgumentLoader.ArgumentSpec(
                        idx, param.name, many, resource_type
                    )
                    self._action_2_specs[action].append(spec)

    def load(self, action: 'Action', args: Tuple, kwargs: Dict) -> Tuple:
        """
        Replace args and kwargs with corresponding Entity and return them
        """
        loaded_args = list(args)
        loaded_kwargs = kwargs.copy()

        for spec in self._action_2_specs[action]:
            pos = spec.position
            posindex = pos-1
            if pos is not None and pos <= len(args):
                raw_arg_value = args[posindex]
                loaded_args_or_kwargs = loaded_args
                key = posindex
                is_positional = True
            else:
                raw_arg_value = kwargs.get(spec.arg_name)
                loaded_args_or_kwargs = loaded_kwargs
                key = spec.arg_name
                is_positional = False

            # Note that "key" is either a position integer offset
            # of the name of a keyword argument.

            loaded_entity = self.load_param(
                spec.many, spec.resource_type, raw_arg_value
            )
            # store a reference to the raw argument value on the loaded Entity
            # so that it can still be accessed inside the app.
            if loaded_entity is not None:
                loaded_entity.internal.arg = raw_arg_value

            loaded_entity = self._on_load(
                spec, raw_arg_value, loaded_entity
            )

            if is_positional:
                loaded_args[key] = loaded_entity
            else:
                loaded_kwargs[key] = loaded_entity

        return (tuple(loaded_args), loaded_kwargs)

    def load_param(self, many: bool, resource_type: Type['Resource'], preloaded):
        """
        Convert the given parameter "preloaded" into its corresponding
        Entity.

        - If the preloaded value is an ID, fetch the object.
        - If it is a list of IDs, return a Batch.
        - If it is a dict, replace it with a corresponding Resource instance.
        """
        if not resource_type:
            return preloaded
        elif not many:
            if is_resource(preloaded):
                return preloaded
            elif isinstance(preloaded, dict):
                if 'id' in preloaded:
                    preloaded[ID] = preloaded.pop('id')
                if 'rev' in preloaded:
                    preloaded[REV] = preloaded.pop('rev')
                self.preload_resolver_state(resource_type, preloaded)
                return resource_type(state=preloaded)
            elif (
                (preloaded and isinstance(preloaded, str)) and
                (preloaded[0] == '{' and preloaded[-1] == '}')
            ):
                try:
                    data = self._app.json.decode(preloaded)
                    return resource_type(data)
                except ValueError:
                    return None
            else:
                return resource_type.get(_id=preloaded)
        elif is_sequence(preloaded):
            if isinstance(preloaded, set):
                preloaded = list(preloaded)
            if is_resource(preloaded[0]):
                return resource_type.Batch(resources=preloaded)
            elif isinstance(preloaded[0], dict):
                return resource_type.Batch(
                    resource_type(
                        state=self.preload_resolver_state(resource_type, record)
                    ).merge(
                        _id=None, _rev=None
                    )
                    for record in preloaded
                )
            else:
                return resource_type.get_many(_ids=preloaded)

    def preload_resolver_state(
        self, resource_type: Type['Resource'], preloaded: Dict
    ):
        # remove any items in the preloaded dict if the corresponding field is
        # designated "protected".
        for k in resource_type.ravel.protected_field_names:
            if k in preloaded:
                del preloaded[k]

        for resolver in resource_type.ravel.resolvers.values():
            if resolver.name in resource_type.ravel.resolvers.fields:
                continue
            preloaded_obj = preloaded.get(resolver.name)
            if preloaded_obj:
                target_resource_class = resolver.target
                target_batch_class = target_resource_class.Batch
                if resolver.many:
                    dict_list = preloaded_obj
                    preloaded[resolver.name] = target_batch_class(
                        target_resource_class(
                            state=self.preload_resolver_state(resolver.target, x)
                        ) for x in dict_list
                    )
                else:
                    state_dict = preloaded_obj
                    preloaded[resolver.name] = target_resource_class(
                        state=self.preload_resolver_state(resolver.target, state_dict)
                    )

        return preloaded
