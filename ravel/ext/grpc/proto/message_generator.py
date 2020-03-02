import sys

from typing import Text, List, Dict, Type

from google.protobuf.message import Message
from appyratus.schema import Schema, fields

from ravel.exceptions import RavelError
from ravel.util.misc_functions import get_class_name



class ActionError(RavelError):
    """
    An `Error` simply keeps a record of an Exception that occurs and the
    middleware object which raised it, provided there was one. This is used
    in the processing of requests, where the existence of errors has
    implications on control flow for middleware.
    """

    def __init__(self, exc, middleware=None):
        self.middleware = middleware
        self.timestamp = TimeUtils.utc_now()
        self.exc = exc
        if isinstance(exc, RavelError) and exc.wrapped_traceback:
            self.trace = self.exc.wrapped_traceback.strip().split('\n')[1:]
            self.exc_message = self.trace.pop().split(': ', 1)[1]
        else:
            self.trace = traceback.format_exc().strip().split('\n')[1:]
            self.exc_message = self.trace.pop().split(': ', 1)[1]

    def to_dict(self):
        data = {
            'timestamp': self.timestamp.isoformat(),
            'message': self.exc_message,
            'traceback': self.trace,
        }

        if isinstance(self.exc, RavelError):
            if not self.exc.wrapped_exception:
                data['exception'] = get_class_name(self.exc)
            else:
                data['exception'] = get_class_name(self.exc.wrapped_exception)

            if self.exc.logged_traceback_depth is not None:
                depth = self.exc.logged_traceback_depth
                data['traceback'] = data['traceback'][-2*depth:]

        if self.middleware is not None:
            data['middleware'] = get_class_name(self.middleware)

        if isinstance(self.exc, RavelError):
            data.update(self.exc.data)

        return data


class BadRequest(RavelError):
    """
    If any exceptions are raised during the execution of Middleware or an
    action callable itself, we raise BadRequest.
    """

    def __init__(self,
        action: 'Action',
        state: 'ExecutionState',
        *args,
        **kwargs
    ):
        super().__init__(
            f'error/s occured in action '
            f'"{action.name}" (see logs)'
        )

        self.action = action


from .field_adapters import (
    FieldAdapter,
    SchemaFieldAdapter,
    NestedFieldAdapter,
    ScalarFieldAdapter,
    ArrayFieldAdapter,
)


class MessageGenerator(object):
    def __init__(self, adapters: Dict[Type[fields.Field], FieldAdapter]=None):
        # default field adapters indexed by appyratus schema Field types
        self.adapters = {
            Schema: SchemaFieldAdapter(),
            fields.Nested: NestedFieldAdapter(),
            fields.String: ScalarFieldAdapter('string'),
            fields.FormatString: ScalarFieldAdapter('string'),
            fields.Email: ScalarFieldAdapter('string'),
            fields.Uuid: ScalarFieldAdapter('string'),
            fields.Bool: ScalarFieldAdapter('bool'),
            fields.Float: ScalarFieldAdapter('double'),
            fields.Int: ScalarFieldAdapter('uint64'),
            fields.DateTime: ScalarFieldAdapter('uint64'),
            fields.Dict: ScalarFieldAdapter('bytes'),
            fields.List: ArrayFieldAdapter(),
            fields.Set: ArrayFieldAdapter(),
            # XXX redundant to List?  does not exist in schema.fields
            #fields.Array: ArrayFieldAdapter(),
            # XXX do we add?  does not exist in schema.fields
            #fields.Enum: EnumFieldAdapter(),
            # XXX do we add?  does not exist in schema.fields
            #fields.Regexp: ScalarFieldAdapter('string'),
        }
        # upsert into default adapters dict from the `adapters` kwarg
        for field_type, adapter in (adapters or {}).items():
            self.adapters[field_type] = adapter
        # associate the generator with each adapter.
        for adapter in self.adapters.values():
            adapter.bind(self)

    def get_adapter(self, field) -> FieldAdapter:
        if isinstance(field, Schema):
            adapter = self.adapters[Schema]
        else:
            adapter = self.adapters.get(type(field))
            if adapter is None:
                for field_type, adapter in self.adapters.items():
                    if issubclass(type(field), field_type):
                        break
            if adapter is None:
                raise Exception(f'adapter for field {field} not found')
            return adapter

    def emit(
        self,
        schema_type: Type['Schema'],
        type_name: Text = None,
        depth=1
    ) -> Text:
        """
        Recursively generate a protocol buffer message type declaration string
        from a given Schema class.
        """
        if isinstance(schema_type, (type, Schema)):
            type_name = type_name or get_class_name(schema_type)
        else:
            raise ValueError(
                'unrecognized schema type: "{}"'.format(schema_type)
            )

        field_no2field = {}
        prepared_data = []
        field_decls = []

        for f in schema_type.fields.values():
            # compute the "field number"
            field_no = f.meta.get('field_no', sys.maxsize)
            if field_no is None:
                raise ValueError(f'f has no protobuf field number')

            # get the field adapter
            adapter = self.get_adapter(f)
            if not adapter:
                raise Exception('no adapter for type {}'.format(type(f)))

            # store in intermediate data structure for purpose of sorting by
            # field numbers
            prepared_data.append((field_no, f, adapter))

        # emit field declarations in order of field number ASC
        sorted_data = sorted(prepared_data, key=lambda x: x[0])
        for (field_no, field, adapter) in sorted_data:
            field_decl = adapter.emit(field, field_no)
            field_decls.append(('  ' * depth) + field_decl + ';')

        nested_message_types = [
            self.emit(nested_schema, depth=depth + 1)
            for nested_schema in {
                type(s) for s in schema_type.children
            }
        ]

        MESSAGE_TYPE_FSTR = (
            (('   ' * (depth - 1)) + '''message {type_name} ''') + \
            ('''{{\n{nested_message_types}{field_lines}\n''') + \
            (('   ' * (depth - 1)) + '''}}''')
        )

        # emit the message declaration "message Foo { ... }"
        return MESSAGE_TYPE_FSTR.format(
            type_name=type_name,
            nested_message_types=(
                '\n'.join(nested_message_types) + '\n'
                if nested_message_types else ''
            ),
            field_lines='\n'.join(field_decls),
        ).rstrip()
