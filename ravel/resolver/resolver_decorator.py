from typing import Text, Type, Union, Callable

from ravel.util.misc_functions import get_callable_name

class ResolverDecorator(object):
    """
    Instances of ResolverDecorator are intended to register Resource instance
    methods as callback function arguments to a Resolver, which is constructed
    by the Resource metaclass.
    """

    def __init__(
        self,
        resolver_type: Type['Resolver'],
        *args,
        **kwargs
    ):
        self.resolver_type = resolver_type
        self.args = args
        self.kwargs = kwargs

    def __call__(self, func: Callable) -> 'ResolverDecorator':
        """
        The decorator's main purpose is to register a an instance method
        as a resolver's on_resolve callback, which is the main function of a
        Resolver.
        """
        self.kwargs['on_resolve'] = func
        self.kwargs.setdefault('name', get_callable_name(func))
        return self

    def __getattr__(self, key):
        """
        This allows the decorator object to have attributes that are themselves
        decorators, for the purpose of adding the decorated methods as
        kwargs to the resolver, with usage like:

        ```python

        @resolver(Account)
        def account(self, request):
            return Query(request=request).execute(first=True)

        @account.on_get
        def log_account_access(self, account):
            print(f'{account._id} accessed')
        ```
        """
        def insert(func):
            func.__name__ = f'{func.__name__}_{key}'
            self.kwargs[key] = func
            return self

        return insert

    def build_property(
        self, owner: Type['Resource'], name: Text
    ) -> 'ResolverProperty':
        """
        Return a new DecoratorProperty using the args and kwargs collected in
        this ResolverDecorator.
        """
        self.kwargs.update({'owner': owner, 'name': name})
        return self.resolver_type.build_property(
            decorator=self, args=self.args, kwargs=self.kwargs
        )
