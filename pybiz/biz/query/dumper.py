from typing import Dict


class QueryDumper(object):
    def dump(self, query: 'Query') -> Dict:
        """
        Recursively convert a Query object into a dict, consisting only of
        Python primitives.
        """
