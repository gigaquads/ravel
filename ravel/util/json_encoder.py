from appyratus.json import JsonEncoder as BaseJsonEncoder

from ravel.util import is_resource, is_batch


class JsonEncoder(BaseJsonEncoder):
    def default(self, target):
        if is_resource(target) or is_batch(target):
            return target.dump()
        else:
            return super().default(target)
