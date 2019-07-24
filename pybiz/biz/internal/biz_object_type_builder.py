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

from .relationship_property import RelationshipProperty
from .field_property import FieldProperty
from ..view import View, ViewProperty
from ..biz_list import BizList, BizListTypeBuilder
from ..biz_attribute import BizAttribute

# TODO: call getmembers only once

class BizObjectTypeBuilder(object):

    biz_list_type_builder = BizListTypeBuilder()

    @classmethod
    def get_instance(cls):
        if not hasattr(cls, '_instance'):
            cls._instance = cls()
        return cls._instance

    def prepare_class_attributes(self, name, bases, ns):
        ns[IS_BIZOBJ_ANNOTATION] = True
        ns[IS_BOOTSTRAPPED] = False

        ns['relationships'] = self._inherit_relationships(bases, ns)
        ns['views'] = self._inherit_views(bases, ns)

        if '__abstract__' in ns:
            static_method = ns.pop('__abstract__')
            is_abstract = static_method.__func__
            ns[IS_ABSTRACT_ANNOTATION] = is_abstract()
        else:
            ns[IS_ABSTRACT_ANNOTATION] = False

        return ns

    def initialize_class_attributes(self, name, biz_type):
        biz_type.Schema = self._build_schema_type(name, biz_type)

        biz_type.schema = biz_type.Schema()
        biz_type.defaults = self._extract_defaults(biz_type)

        self._build_field_properties(biz_type)
        self._build_relationship_properties(biz_type)
        self._build_view_properties(biz_type)
        self._aggregate_selectable_attribute_names(biz_type)

        biz_type.BizList = self.biz_list_type_builder.build(biz_type)

        # TODO: build in support for creating generic BizAttribute properties
        # and storing them somewhere here, thne updating the logic in BizList

        setattr(biz_type, 'r', DictObject(biz_type.relationships))
        setattr(biz_type, 'f', DictObject(biz_type.schema.fields))
        setattr(biz_type, 'v', DictObject(biz_type.views))

        console.debug(
            message=f'{biz_type.__name__} fields:',
            data={'fields': list(biz_type.schema.fields.keys())}
        )

        if biz_type.relationships:
            console.debug(
                message=f'{biz_type.__name__} relationships:',
                data={'relationships': list(biz_type.relationships.keys())}
            )

    def _aggregate_selectable_attribute_names(self, biz_type):
        biz_type.selectable_attribute_names = set()
        selectable_types = (BizAttribute, RelationshipProperty, FieldProperty)
        is_selectable = lambda x: isinstance(x, selectable_types)
        for k, v in inspect.getmembers(biz_type, predicate=is_selectable):
            biz_type.selectable_attribute_names.add(k)

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

    def _inherit_relationships(self, bases: Tuple[Type], ns: Dict) -> Dict:
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
        for rel_name, rel in biz_type.relationships.items():
            rel_prop = RelationshipProperty(rel)
            rel.associate(biz_type, rel_name)
            setattr(biz_type, rel_name, rel_prop)

    def _inherit_views(self, bases: Tuple[Type], ns: Dict) -> Dict:
        views = {}

        for k, v in list(ns.items()):
            if isinstance(v, View):
                views[k] = v
                del ns[k]

        for base in bases:
            if is_bizobj(base):
                inherited_views = getattr(base, 'views', {})
            else:
                inherited_views = {
                    k: v for k, v in inspect.getmembers(
                        base, predicate=lambda v: isinstance(v, View)
                    )
                }
            for k, v in inherited_views.items():
                views[k] = copy.deepcopy(v)

        return views

    def _build_view_properties(self, biz_type):
        for view_name, view in biz_type.views.items():
            view_prop = ViewProperty.build(view)
            view.associate(biz_type, view_name)
            setattr(biz_type, view.name, view_prop)
