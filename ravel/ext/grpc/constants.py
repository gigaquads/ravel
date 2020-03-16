DEFAULT_HOST = '127.0.0.1'
DEFAULT_PORT = '50051'
DEFAULT_GRACE = 5.0
DEFAULT_IS_SECURE = False
REGISTRY_PROTO_FILE = 'app.proto'
PB2_MODULE_PATH_FSTR = '{}.grpc.app_pb2'
PB2_GRPC_MODULE_PATH_FSTR = '{}.grpc.app_pb2_grpc'
PROTOC_COMMAND_FSTR = '''
python3 -m grpc_tools.protoc
    -I {include_dir}
    --python_out={build_dir}
    --grpc_python_out={build_dir}
    {proto_file}
'''
PROTOC_WEB_GENERATE_MESSAGE_CLASSES_COMMAND = '''
protoc 
    -I {include_dir}
    --js_out=import_style=commonjs:{build_dir}
    {proto_file}
'''
PROTOC_WEB_GENERATE_CLIENT_COMMAND = '''
protoc
    -I {include_dir}
    --grpc-web_out=import_style=commonjs,mode=grpcwebtext:{build_dir}
    {proto_file}
'''