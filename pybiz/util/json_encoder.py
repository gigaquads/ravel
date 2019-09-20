from appyratus.json import JsonEncoder as BaseJsonEncoder

from .misc_functions import is_biz_obj, is_biz_list


class JsonEncoder(BaseJsonEncoder):
    def default(self, target):
        if is_biz_obj(target) or is_biz_list(target):
            return target.dump()
        else:
            return super().default(target)
