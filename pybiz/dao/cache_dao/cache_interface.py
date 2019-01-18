from typing import Dict, Set


class CacheInterface(object):
    def fetch_cache(self, _ids: Set, rev=True, data=False, fields: Set = None) -> Dict:
        raise NotImplementedError()
