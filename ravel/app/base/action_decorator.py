from typing import Union, Type
from inspect import getmembers, ismethod

from .action import Action


class ActionDecorator(object):
    def __init__(self, app: 'Application', *args, **kwargs):
        super().__init__()
        self.app = app
        self.args = args
        self.kwargs = kwargs
        self._api_object = None

    def __call__(self, obj) -> Union['Action', Type]:
        if isinstance(obj, type):
            # interpret all non-private methods as action functions
            # and register them all
            api_type = obj
            self._api_object = api_type(self.app)
            predicate = lambda x: (
                (ismethod(x) and x.__name__[0] != '_')
                or isinstance(x, Action)
            )
            for k, v in getmembers(self.api_object, predicate=predicate):
                if isinstance(v, Action):
                    # customize existing action
                    existing_action = self.app.actions.get(v.name)
                    if not existing_action:
                        # it's an action but for a different Application
                        # i.e. existing_action.app is not self.app
                        continue

                    kwargs = self.kwargs.copy()
                    kwargs.update(existing_action.decorator.kwargs)

                    new_decorator = type(self)(self.app, *self.args, **kwargs)
                    new_decorator._api_object = self._api_object
                    new_decorator.setup_action(v.target, True)
                else:
                    self.setup_action(v.__func__, False)
            return api_type
        else:
            func = obj
            action = self.setup_action(func, False)
            func._ravel_action = action
            return func

    def setup_action(self, func, overwrite):
        action = self.app.action_type(func, self)
        self.app.add_action(action, overwrite=overwrite)
        self.app.on_decorate(action)
        return action

    @property
    def api_object(self) -> 'Api':
        return self._api_object
