import  re


ROOT_ATTR = '/'

PATCH_PATH_ANNOTATION = '_pre_patch'
PRE_PATCH_ANNOTATION = '_pre_patch'
POST_PATCH_ANNOTATION = '_post_patch'
PATCH_ANNOTATION = '_patch'
IS_BIZOBJ_ANNOTATION = 'is_bizobj'

HTTP_GET = 'GET'
HTTP_POST = 'POST'
HTTP_PUT = 'PUT'
HTTP_PATCH = 'PATCH'
HTTP_DELETE = 'DELETE'

OP_LOAD = 'load'
OP_DUMP = 'dump'

RE_EMAIL = re.compile(r'^[a-f]\w+(\.\w+)?@\w+\.\w+$', re.I)
RE_FLOAT = re.compile(r'^\d*(\.\d*)?$')
RE_UUID = re.compile(r'^[a-f0-9]{32}$')
