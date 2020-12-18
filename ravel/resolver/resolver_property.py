from typing import Tuple, Text

from ravel.util.loggers import console
from ravel.util.misc_functions import get_class_name
from ravel.util import is_resource, is_batch, is_sequence
from ravel.query.order_by import OrderBy
from ravel.query.request import Request

DNE = {}


class ResolverProperty(property):
    """
    # ResolverProperty
    The resolvers declared on resource classes are dynamically converted into resolver
    properties at runtime. Resolving, reading, and writing resource instance state is
    managed through ResolverDecorator.

    ## Lazy Loading
    If necessary, ResolverProperty lazy loads its value on "get" by executing
    its resolver's resolve method.

    ## Resolver CRUD Callbacks
    The getter, setter, and deleter of ResolverProperty execute its resolver's
    on_get, on_set, and on_delete callbacks.
    """

    def __init__(
        self,
        resolver: 'Resolver',
        decorator: 'ResolverDecorator' = None
    ):
        """
        Args:
        - `resolver`: The resolver managed by this property.
        - `decorator`: The ResolverDecorator used to build the resolver, if any.
        """
        super().__init__(fget=self.fget, fset=self.fset, fdel=self.fdel)
        self.decorator = decorator
        self.resolver = resolver

    @property
    def app(self) -> 'Application':
        """
        The host Application for this property's resolver.
        """
        return self.resolver.owner.ravel.app

    def select(self, *items: Tuple):
        """
        Build and return a query request. This is used in query-building syntax.
        It's used like this:

        ```python
        account_request = User.account.select(Account._id)
        query = User.select(User._id, request)

        # or more succinctly:
        query = User.select(
            User._id,
            User.account.select(Account._id),
        )
        ```
        """
        request = Request(self.resolver)
        request.select(*items)
        self.resolver.on_select(request)
        return request

    def fget(self, resource: 'Resource'):
        """
        Returns resource state data associated with the resolver. For example,
        if the name of the resolver were "email", corresponding to an email
        field, we would return `resource.state["email"]`. However, if "email" is
        missing from the state dict, then we lazy load it through the resolver,
        memoizing it in the state dict. This method also runs the resolver's
        on_get callback.
        """
        resolver = self.resolver

        # if value not loaded, lazily resolve it
        if resolver.name not in resource.internal.state:
            console.debug(f'lazy loading {resource}.{resolver.name}')
            request = Request(resolver)
            value = resolver.resolve(resource, request)

            # if resolver is for a field, we know that the resolved
            # field has been loaded ON the returned resource (value)
            if resolver.name in resource.ravel.resolvers.fields:
                value = resource.internal.state.get(resolver.name, DNE)
                if value is DNE:
                    console.debug(
                        message=(
                            f'no value returned for '
                            f'{resource}.{resolver.name}'
                        )
                    )
                    return

            if (value is not None) or resolver.nullable:
                resource.internal.state[resolver.name] = value
            elif (value is None) and (not resolver.nullable):
                if resource.internal.state.get(resolver.name) is None:
                    resource.internal.state.pop(resolver.name, None)
                console.warning(
                    message=(
                        f'resolver returned bad value'
                    ),
                    data={
                        'resource': resource._id,
                        'class': resource.class_name,
                        'resolver': self.resolver.name,
                        'reason': 'resolver not nullable',
                    }
                )

        value = resource.internal.state.get(resolver.name)
        resolver.on_get(resource, value)
        return value

    def fset(self, resource: 'Resource', new_value):
        """
        Set resource state data, calling the resolver's on_set callbak.
        """
        resolver = self.resolver
        target_class = self.resolver.target
        target_schema = target_class.ravel.schema
        old_value = resource.internal.state.get(resolver.name)

        # cast plain lists and dicts to target Resource or Batch objects
        if self.resolver.many:
            if not is_batch(new_value):
                assert is_sequence(new_value)
                new_value = self.resolver.target.Batch(
                    target_class(x) if isinstance(x, dict) else x
                    for x in new_value
                )
        elif isinstance(new_value, dict) \
                and resolver.name not in target_schema.fields:
            new_value = self.resolver.target(new_value)

        # write new value to instance state
        resource.internal.state[resolver.name] = new_value

        # trigger on_set callback
        resolver.on_set(resource, old_value, new_value)

    def fdel(self, resource: 'Resource'):
        """
        Remove state data from the resource's state dict, calling the resolver's
        on_delete callback.
        """
        resolver = self.resolver
        if resolver.name in resource.internal.state:
            old_value = resource.internal.state.pop(resolver.name)
            resolver.on_delete(resource, old_value)
