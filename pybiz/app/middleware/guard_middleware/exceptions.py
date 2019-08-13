from pybiz.exceptions import PybizError


class GuardFailed(PybizError):
    def __init__(self, guard):
        super().__init__(f'failed guard: {guard.display_string}')
        self.guard = guard
