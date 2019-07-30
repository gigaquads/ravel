from appyratus.json import JsonEncoder as BaseJsonEncoder

from .misc_functions import is_bizobj, is_bizlist


class JsonEncoder(BaseJsonEncoder):
    def default(self, target):
        if is_bizobj(target) or is_bizlist(target):
            return target.dump()
        else:
            return super().default(target)
