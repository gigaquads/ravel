from typing import Text

from appyratus.memoize import memoized_property


class Relationship(object):
    """
    - If any relationship name matches a schema field name of type <List> or
      <Object>, try to load the raw data into relationship data.

    """
    def __init__(
        self,
        target,
        many=False,
        source: Text = None,
        query=None,
        lazy=True,
        private=False,
    ):
        self._target = target
        self.source = source
        self.many = many
        self.query = query
        self.name = None
        self.lazy = lazy
        self.private = private

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

    @classmethod
    def build(
        cls,
        relationship: 'Relationship'
    ) -> 'RelationshipProperty':
        """
        Build and return a `RelationshipProperty`, that validates the data on
        getting/setting and lazy-loads data on get.
        """
        rel = relationship
        key = relationship.name

        def is_scalar_value(obj):
            # just a helper func
            return not isinstance(obj, (list, set, tuple))

        def fget(self):
            """
            Return the related BizObject instance or list.
            """
            if key not in self._related:
                if rel.lazy and rel.query:
                    # lazily fetch the related data, eagerly selecting all fields
                    related_obj = rel.query(self, {'*'})
                    # make sure we are setting an instance object or collection
                    # of objects according to the field's "many" flag.
                    is_scalar = is_scalar_value(related_obj)
                    expect_scalar = not rel.many
                    if is_scalar and not expect_scalar:
                        raise ValueError(
                            'relationship "{}" query returned an object but '
                            'expected a sequence because relationship.many '
                            'is True'.format(key)
                        )
                    elif (not is_scalar) and expect_scalar:
                        raise ValueError(
                            'relationship "{}" query returned a sequence but '
                            'expected a BizObject because relationship.many '
                            'is False'.format(key)
                        )
                    self._related[key] = related_obj

            default = [] if rel.many else None
            return self._related.get(key, default)

        def fset(self, value):
            """
            Set the related BizObject or list, enuring that a list can't be
            assigned to a Relationship with many == False and vice versa.
            """
            rel = self.relationships[key]
            is_scalar = is_scalar_value(value)
            expect_scalar = not rel.many

            if (not expect_scalar) and isinstance(value, dict):
                # assume that the value is a map from id to bizobj, so
                # convert the dict value set into a list to use as the
                # value set for the Relationship.
                value = list(value.values())

            if is_scalar and not expect_scalar:
                    raise ValueError(
                        'relationship "{}" must be a sequence because '
                        'relationship.many is True'.format(key)
                    )
            elif (not is_scalar) and expect_scalar:
                raise ValueError(
                    'relationship "{}" cannot be a BizObject because '
                    'relationship.many is False'.format(key)
                )
            self._related[key] = value

        def fdel(self):
            """
            Remove the related BizObject or list. The field will appeear in
            dump() results. You must assign None if you want to None to appear.
            """
            del self._related[k]

        return cls(relationship, fget=fget, fset=fset, fdel=fdel)

