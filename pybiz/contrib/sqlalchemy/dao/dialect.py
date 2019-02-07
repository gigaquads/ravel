from appyratus.enum import EnumValueStr


class Dialect(EnumValueStr):
    @staticmethod
    def values():
        return {
            'postgresql',
            'mysql',
            'sqlite',
        }
