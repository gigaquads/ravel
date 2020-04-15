from typing import Text

from ravel.util.misc_functions import get_class_name


class ParameterAssignment(object):
    """
    This is an internal data structure, used to facilitate the syntactic sugar
    that allows you to write to query.params via funcion call notation, like
    query.foo('bar') instead of query.params['foo'] = bar.

    Instances of this class just store the query whose parameter we are going to
    set and the name of the dict key or "param name". When called, it writes the
    single argument supplied in the call to the params dict of the query.
    """

    def __init__(self, owner, name: Text):
        self.owner = owner
        self.name = name

    def __call__(self, value):
        """
        Store the `param` value in the Query's parameters dict.
        """
        self.owner.parameters[self.name] = value
        return self.owner

    def __repr__(self):
        return (
            f'{get_class_name(self)}('
            f'parameter={self.name}'
            f')'
        )
