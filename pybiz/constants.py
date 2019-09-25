import os

ID_FIELD_NAME = '_id'
REV_FIELD_NAME = '_rev'

IS_BIZ_OBJECT_ANNOTATION = 'pybiz_is_biz_object'
IS_BIZ_LIST_ANNOTATION = 'pybiz_is_biz_list'
CONSOLE_LOG_LEVEL = os.environ.get('PYBIZ_CONSOLE_LOG_LEVEL', 'DEBUG')
