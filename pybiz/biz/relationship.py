import copy

from typing import Text, List, Tuple

from appyratus.memoize import memoized_property
from appyratus.schema import fields
from appyratus.schema.fields import Field

from pybiz.predicate import (
    Predicate,
    ConditionalPredicate,
    BooleanPredicate,
)


class Relationship(object):
    """
    - If any relationship name matches a schema field name of type <List> or
      <Object>, try to load the raw data into relationship data.

    """
    def __init__(
        self,
        target,
        many=False,
        predicate: Predicate = None,
        link: Text = None,
        source: Text = None,
        query=None,
    ):
        self._target = target
        self.source = source
        self.predicate = predicate
        self.link = link or '_id'
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


class RelationshipProperty(property):
    def __init__(self, relationship, **kwargs):
        super().__init__(**kwargs)
        self.relationship = relationship
