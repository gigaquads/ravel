from pybiz.exceptions import PybizError
from pybiz.util.misc_functions import get_class_name


class GuardFailure(PybizError):
    def __init__(self, guard, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.guard = guard
        self.data['guard'] = {}
        self.data['guard']['class'] = get_class_name(guard)
        self.data['guard']['description'] = guard.description
        if guard is not guard.root:
            self.data['guard']['owner'] = guard.root.description
