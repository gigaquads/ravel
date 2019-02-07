from typing import Text, List

from appyratus.schema import Schema
from appyratus.utils import StringUtils

from ..registry import Registry, RegistryProxy
from .proto import MessageGenerator


class GrpcRegistryProxy(RegistryProxy):
    def __init__(self, func, decorator):
        def build_schema(kwarg, name_suffix):
            type_name = self._msg_name_prefix + name_suffix
            if isinstance(kwarg, dict):
                return Schema.factory(type_name, kwarg)()
            else:
                return kwarg

        super().__init__(func, decorator)

        self.msg_gen = MessageGenerator()
        self._msg_name_prefix = decorator.kwargs.get('message_name_prefix')
        if self._msg_name_prefix is None:
            self._msg_name_prefix = StringUtils.camel(self.name)

        self.request_schema = build_schema(
            decorator.kwargs.get('request'), 'Request'
        )
        self.response_schema = build_schema(
            decorator.kwargs.get('response'), 'Response'
        )

    def __call__(self, *raw_args, **raw_kwargs):
        return super().__call__(*(raw_args[:1]), **raw_kwargs)

    def generate_protobuf_message_types(self) -> List[Text]:
        return [
            self.msg_gen.emit(self.request_schema),
            self.msg_gen.emit(self.response_schema),
        ]

    def generate_protobuf_function_declaration(self) -> Text:
        return (
            'rpc {func_name}({req_msg_type}) '
            'returns ({resp_msg_type})'
            ' {{}}'.format(
                func_name=self.name,
                req_msg_type=self._msg_name_prefix + 'Request',
                resp_msg_type=self._msg_name_prefix + 'Response',
            )
        )
