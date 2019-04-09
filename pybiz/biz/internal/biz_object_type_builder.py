import copy
import inspect

from typing import Type, List, Dict, Tuple

from appyratus.utils import DictObject

from pybiz.biz.relationship.relationship import Relationship
from pybiz.schema import Schema, fields, String, Field, Int
from pybiz.util import import_object, is_bizobj
from pybiz.util.loggers import console
from pybiz.constants import (
    IS_BIZOBJ_ANNOTATION,
    IS_BOOTSTRAPPED,
    IS_ABSTRACT_ANNOTATION,
)

from ..biz_list import BizList
from .relationship_property import RelationshipProperty
from .field_property import FieldProperty


class BizObjectTypeBuilder(object):

    @classmethod
    def get_instance(cls):
        if not hasattr(cls, '_instance'):
            cls._instance = cls()
        return cls._instance

    def prepare_class_attributes(self, name, bases, ns):
        ns['relationships'] = self._build_relationships(bases, ns)
        ns[IS_BIZOBJ_ANNOTATION] = True
        ns[IS_BOOTSTRAPPED] = False

        if '__abstract__' in ns:
            static_method = ns.pop('__abstract__')
            is_abstract = static_method.__func__
            ns[IS_ABSTRACT_ANNOTATION] = is_abstract()
        else:
            ns[IS_ABSTRACT_ANNOTATION] = False

        return ns

    def initialize_class_attributes(self, name, biz_type):
        biz_type.Schema = self._build_schema_type(name, biz_type)
        biz_type.BizList = self._build_biz_list_type(biz_type)
        biz_type.schema = biz_type.Schema()
        biz_type.defaults = self._extract_defaults(biz_type)

        self._bind_relationships(biz_type)
        self._build_relationship_properties(biz_type)
        self._build_field_properties(biz_type)

        setattr(biz_type, 'r', DictObject(biz_type.relationships))
        setattr(biz_type, 'f', DictObject(biz_type.schema.fields))

        console.debug(
            message=f'{biz_type.__name__} fields:',
            data={'fields': list(biz_type.schema.fields.keys())}
        )

        if biz_type.relationships:
            console.debug(
                message=f'{biz_type.__name__} relationships:',
                data={'relationships': list(biz_type.relationships.keys())}
            )

    def _build_schema_type(self, name, biz_type):
        # use the schema class override if defined
        obj = biz_type.__schema__()
        if obj:
            if isinstance(obj, str):
                class_name = obj
                schema_type = import_object(class_name)
            elif isinstance(obj, type) and issubclass(obj, Schema):
                schema_type = obj
            else:
                raise ValueError(str(obj))
        else:
            schema_type = None

        fields = copy.deepcopy(schema_type.fields) if schema_type else {}

        # "inherit" fields of parent BizObject.Schema
        inherited_schema_type = getattr(biz_type, 'Schema', None)
        if inherited_schema_type is not None:
            for k, v in inherited_schema_type.fields.items():
                fields.setdefault(k, copy.deepcopy(v))

        # collect and field declared on this BizObject class
        for k, v in inspect.getmembers(
            biz_type, predicate=lambda x: isinstance(x, Field)
        ):
            if k is 'schema':
                # XXX `schema` gets recognized by getmembers as a Field
                continue
            fields[k] = v

        # bless each bizobj with a mandatory _id field.
        if '_id' not in fields:
            fields['_id'] = String(nullable=True)
        if '_rev' not in fields:
            fields['_rev'] = Int(nullable=True)

        return Schema.factory('{}Schema'.format(name), fields)

    def _extract_defaults(self, biz_type: Type['BizObject']) -> Dict:
        # start with inherited defaults
        defaults = copy.deepcopy(getattr(biz_type, 'defaults', {}))

        # add any new defaults from the schema
        for k, field in biz_type.schema.fields.items():
            if field.default is not None:
                defaults[k] = field.default
                field.default = None

        return defaults

    def _build_relationships(self, bases: Tuple[Type], ns: Dict) -> Dict:
        relationships = {}

        for k, v in list(ns.items()):
            if isinstance(v, Relationship):
                relationships[k] = v
                del ns[k]

        for base in bases:
            if is_bizobj(base):
                inherited_relationships = getattr(base, 'relationships', {})
            else:
                inherited_relationships = {
                    k: v for k, v in inspect.getmembers(
                        base, predicate=lambda v: isinstance(v, Relationship)
                    )
                }
            for k, v in inherited_relationships.items():
                relationships[k] = copy.deepcopy(v)

        return relationships

    def _bind_relationships(self, biz_type):
        for k, rel in biz_type.relationships.items():
            rel.associate(biz_type, k)

    def _build_field_properties(self, biz_type):
        """
        Create properties out of the fields declared on the schema associated
        with the class.
        """
        for field_name, field in biz_type.schema.fields.items():
            if field_name not in biz_type.relationships:
                field_prop = FieldProperty.build(biz_type, field)
                setattr(biz_type, field_name, field_prop)

    def _build_relationship_properties(self, biz_type):
        for rel in biz_type.relationships.values():
            rel_prop = RelationshipProperty.build(rel)
            setattr(biz_type, rel.name, rel_prop)

    def _build_biz_list_type(self, biz_type):
        return BizList.type_factory(biz_type)
