from typing import Type

import pybiz.biz.resolver.resolver as resolver_module


class ResolverDecorator(object):
    """
    This is the "@resolver" in code, like:

    ```py
    @resolver
    def account(self, resolver):
        return Account.select().where(_id=self.account_id).execute(first=True)

    @account.on_get
    def log_account_access(self, resolver, account):
        log_access(self, account)
    ```

    The job of the decorator is to collect together all arguments required by
    the constructor of the Resolver type (self.resolver_class).
    """

    def __init__(self, resolver: Type['Resolver'] = None, target=None, **kwargs):
        Resolver = resolver_module.Resolver

        if target is not None:
            kwargs['target_biz_class'] = target

        if not isinstance(resolver, type):
            # in this case, the decorator was used like "@resolver"
            self.resolver_class = Resolver
            self.on_execute_func = resolver
            self.name = resolver.__name__
        else:
            # otherwise, it was used like "@resolver()"
            self.resolver_class = resolver or Resolver
            assert isinstance(self.resolver_class, type)
            self.on_execute_func = None
            self.name = None

        self.kwargs = kwargs.copy()
        self.on_get_func = None
        self.on_set_func = None
        self.on_del_func = None

    def __call__(self, func=None):
        # logic to perform lazily if this decorator was created
        # like @resolver() instead of @resolver:
        if func is not None:
            self.on_execute_func = func
            self.name = func.__name__

        return self

    def on_get(self, func):
        self.on_get_func = func
        return self

    def on_set(self, func):
        self.on_set_func = func
        return self

    def on_del(self, func):
        self.on_del_func = func
        return self
