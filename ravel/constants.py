from appyratus.env import Environment


ENV = Environment()

# logging:
CONSOLE_LOG_LEVEL = ENV.get('RAVEL_CONSOLE_LOG_LEVEL', 'DEBUG')


# field name constants:
ID = '_id'
REV = '_rev'


# entity class annotations used for type checking:
IS_RESOURCE = '_ravel_is_resource'
IS_BATCH = '_ravel_is_batch'
