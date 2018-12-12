import copy

from typing import Text

from appyratus.memoize import memoized_property


class Relationship(object):
    """
    - If any relationship name matches a schema field name
      of type <List> or <Object>, try to load the raw data
      into relationship data.

    """
    def __init__(
        self,
        target,
        many=False,
        dump_to: Text = None,    # TODO: deprecate this kwarg
        load_from: Text = None,  # TODO: deprecate this kwarg
        query=None,
    ):
        self._target = target
        self.load_from = load_from
        self.dump_to = dump_to
        self.many = many
        self.query = query
        self.name = None

    @memoized_property
    def target(self):
        """
        Target is expected to be a class object. If the `target` arg passed into
        the ctor is a callable, then we lazy load the class object here from the
        return value.
        """
        if (not isinstance(self._target, type) and callable(self._target)):
            return self._target()
        else:
            return self._target

    def copy(self):
        return copy.deepcopy(self)


class RelationshipProperty(property):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
