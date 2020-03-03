import socket
import pickle
import codecs

from typing import Text, List, Dict, Type, Union

from google.protobuf.message import Message
from appyratus.schema import Schema, fields
from appyratus.utils import StringUtils

from ravel.util import is_resource, is_batch, get_class_name


def touch_file(filepath: Text):
    """
    Ensure a file exists at the given file path, creating one if does not
    exist.
    """
    with open(os.path.join(filepath), 'a'):
        pass


def is_port_in_use(addr) -> bool:
    """
    Utility method for determining if the server address is already in use.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        host, port_str = addr.split(':')
        sock.bind((host, int(port_str)))
        return False
    except OSError as err:
        if err.errno == 48:
            return True
        else:
            raise err
    finally:
        sock.close()


def bind_message(message: Message, source: Dict) -> Message:
    if source is None:
        return None

    for k, v in source.items():
        if isinstance(getattr(message, k), Message):
            sub_message = getattr(message, k)
            assert isinstance(v, dict)
            bind_message(sub_message, v)
        elif isinstance(v, dict):
            v_bytes = codecs.encode(pickle.dumps(v), 'base64')
            setattr(message, k, v_bytes)
        elif isinstance(v, (list, tuple, set)):
            list_field = getattr(message, k)
            sub_message_type_name = (
                '{}Schema'.format(k.title().replace('_', ''))
            )
            sub_message_type = getattr(message, sub_message_type_name, None)
            if sub_message_type:
                list_field.extend(
                    bind_message(sub_message_type(), v_i)
                    for v_i in v
                )
            else:
                v_prime = [
                    x if not isinstance(x, dict)
                        else codecs.encode(pickle.dumps(x), 'base64')
                    for x in v
                ]
                list_field.extend(v_prime)
        else:
            setattr(message, k, v)

    return message


def dump_result_obj(obj) -> Union[Dict, List]:
    if is_resource(obj):
        return obj.internal.state
    elif is_batch(obj):
        return [x.internal.state for x in obj]
    elif isinstance(obj, (list, set, tuple)):
        return [dump_result_obj(x) for x in obj]
    elif isinstance(obj, dict):
        return {k: dump_result_obj(v) for k, v in obj.items()}
    else:
        return obj
