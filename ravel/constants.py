import os


ID_FIELD_NAME = '_id'
REV_FIELD_NAME = '_rev'

ABSTRACT_MAGIC_METHOD = '__abstract__'
IS_RESOURCE_ATTRIBUTE = 'ravel_is_resource'
IS_BATCH_ATTRIBUTE = 'ravel_is_batch'
CONSOLE_LOG_LEVEL = os.environ.get('PYBIZ_CONSOLE_LOG_LEVEL', 'DEBUG')

def EMPTY_FUNCTION():
    pass
