from typing import Dict


class CacheRecord(object):
    def __init__(self, rev: int = None, data: Dict = None):
        self.rev = rev
        self.data = data
