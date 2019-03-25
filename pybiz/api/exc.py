from pybiz.exc import PybizError


class RegistryError(PybizError):
    pass


class RegistryProxyError(PybizError):
    def __init__(self, errors=None, *args, **kwargs):
        super().__init__('error proxying request')
        self.errors = errors
        print(errors)
        for err in errors:
            print('\n'.join(err.trace))


class GuardFailed(PybizError):
    def __init__(self, guard):
        super().__init__(f'failed guard: {guard.display_string}')
        self.guard = guard
