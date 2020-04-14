from appyratus.env import Environment
from appyratus.enum import Enum


ENV = Environment()

# logging:
CONSOLE_LOG_LEVEL = ENV.get('RAVEL_CONSOLE_LOG_LEVEL', 'INFO')


# field name constants:
ID = '_id'
REV = '_rev'


# entity class annotations used for type checking:
IS_RESOURCE = '_ravel_is_resource'
IS_BATCH = '_ravel_is_batch'


# op codes for Predicate objects
OP_CODE = Enum(
    EQ='eq',
    NEQ='neq',
    GT='gt',
    LT='lt',
    GEQ='geq',
    LEQ='leq',
    INCLUDING='in',
    EXCLUDING='ex',
    AND='and',
    OR='or',
)
