from appyratus.enum import EnumValueStr


class QueryMode(EnumValueStr):
    @staticmethod
    def values():
        return {
            'normal',
            'simulation',
            'backfill',
        }
