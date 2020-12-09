import numpy as np

from ravel.schema import fields


class Array(fields.List):
    def process(self, obj):
        processed_obj, error = super().process(obj)
        if error:
            return (None, error)
        arr = np.array(processed_obj, dtype=self.nested.np_dtype)
        return (arr, None)