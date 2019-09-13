import copy
import inspect
import bisect
import venusian

from typing import Type, List, Dict, Tuple, Text
from collections import defaultdict

from appyratus.utils import DictObject

from pybiz.schema import Schema, fields, String, UuidString, Field, Int, Id
from pybiz.util.misc_functions import import_object, is_bizobj
from pybiz.util.loggers import console
from pybiz.constants import (
    IS_BIZOBJ_ANNOTATION,
    IS_BOOTSTRAPPED,
    IS_ABSTRACT_ANNOTATION,
    ABSTRACT_MAGIC_METHOD,
    ATTRIBUTES_ATTR,
    ID_FIELD_NAME,
    REV_FIELD_NAME,
)

from ..field_property import FieldProperty
from ..biz_list import BizList, BizListTypeBuilder
from ..biz_attribute import BizAttribute, BizAttributeProperty
from ..biz_attribute.relationship import Relationship, RelationshipProperty
from ..biz_attribute.view import View, ViewProperty

# TODO: call getmembers only once

class BizObjectTypeBuilder(object):
    biz_list_type_builder = BizListTypeBuilder()

    def on_new(self, name, bases, namespace):
        namespace[IS_BIZOBJ_ANNOTATION] = True
        namespace[IS_BOOTSTRAPPED] = False
        namespace[IS_ABSTRACT_ANNOTATION] = self._compute_is_abstract(namespace)
        namespace[ATTRIBUTES_ATTR] = self._build_biz_attr_manager(bases, namespace)
        return namespace

    def on_init(self, name, biz_class):
        biz_class.Schema = self._build_schema_class(name, biz_class)

        biz_class.schema = biz_class.Schema()
        biz_class.schema.pybiz_internal = True
        biz_class.defaults = self._extract_defaults(biz_class)

        for field in biz_class.schema.fields.values():
            if not getattr(field, 'pybiz_internal', False):
                field_prop = FieldProperty(biz_class, field)
                setattr(biz_class, field.name, field_prop)

        for biz_attr in biz_class.attributes.values():
            biz_attr.bind(biz_class)
            biz_attr_prop = biz_attr.build_property()
            setattr(biz_class, biz_attr.name, biz_attr_prop)

        # convenient aliases:
        biz_class.relationships = biz_class.attributes.relationships
        biz_class.views = biz_class.attributes.views

        biz_class.selectable_attribute_names = set()
        biz_class.selectable_attribute_names.update(biz_class.schema.fields.keys())
        biz_class.selectable_attribute_names.update(biz_class.attributes.keys())

        biz_class.BizList = self.biz_list_type_builder.build(biz_class)

    def _compute_is_abstract(self, namespace):
        if ABSTRACT_MAGIC_METHOD in namespace:
            static_method = namespace.pop(ABSTRACT_MAGIC_METHOD)
            is_abstract = static_method.__func__
            return is_abstract()
        return False

    def _build_schema_class(self, name, biz_class):
        # use the schema class override if defined
        obj = biz_class.__schema__()
        if obj:
            if isinstance(obj, str):
                class_name = obj
                schema_class = import_object(class_name)
            elif isinstance(obj, type) and issubclass(obj, Schema):
                schema_class = obj
            else:
                raise ValueError(str(obj))
        else:
            schema_class = None

        fields = copy.deepcopy(schema_class.fields) if schema_class else {}

        # "inherit" fields of parent BizObject.Schema
        inherited_schema_class = getattr(biz_class, 'Schema', None)
        if inherited_schema_class is not None:
            for k, v in inherited_schema_class.fields.items():
                fields.setdefault(k, copy.deepcopy(v))

        # collect and field declared on this BizObject class
        is_field = lambda x: isinstance(x, Field)
        for k, v in inspect.getmembers(biz_class, predicate=is_field):
            fields[k] = v

        # bless each bizobj with mandatory built-in Fields
        if ID_FIELD_NAME not in fields:
            fields[ID_FIELD_NAME] = Id(nullable=True)
        if REV_FIELD_NAME not in fields:
            fields[REV_FIELD_NAME] = Int(nullable=True)

        fields.pop('schema', None)

        return Schema.factory(f'{name}Schema', fields)

    def _extract_defaults(self, biz_class: Type['BizObject']) -> Dict:
        # start with inherited defaults
        defaults = copy.deepcopy(getattr(biz_class, 'defaults', {}))

        # add any new defaults from the schema
        for k, field in biz_class.schema.fields.items():
            if field.default is not None:
                defaults[k] = field.default
                field.default = None

        return defaults

    def _build_biz_attr_manager(self, bases: Tuple[Type], ns: Dict) -> Dict:
        manager = BizAttributeManager()

        for base in bases:
            if is_bizobj(base):
                # inherit BizAttributes from base BizObject class
                inherited_manager = getattr(base, 'attributes', {})
                for group, biz_attr in base.attributes.items():
                    if biz_attr.name not in ns:
                        manager.register(group, copy.copy(biz_attr))
            else:
                # inherit BizAttributes declared on a mixin class
                is_biz_attr = lambda v: isinstance(v, BizAttribute)
                for k, v in inspect.getmembers(base, predicate=is_biz_attr):
                    if k not in ns:
                        manager.register(k, copy.copy(v))

        # collect newly defined BizAttributes in the namespace
        # for the new BizObject class being built.
        for k, v in list(ns.items()):
            if isinstance(v, BizAttribute):
                manager.register(k, v)
                del ns[k]

        return manager


class BizAttributeManager(object):
    def __init__(self, *args, **kwargs):
        self._name_2_biz_attr = {}
        self._group_map = defaultdict(dict)
        self._ordered_biz_attrs = []

    def __iter__(self):
        return (biz_attr.name for biz_attr in self._ordered_biz_attrs)

    def __getitem__(self, key):
        return self._name_2_biz_attr[key]

    def __contains__(self, key):
        return key in self._name_2_biz_attr

    def __len__(self):
        return len(self._name_2_biz_attr)

    def keys(self):
        return self._name_2_biz_attr.keys()

    def values(self):
        return self._name_2_biz_attr.values()

    def items(self):
        return (
            (biz_attr.name, biz_attr)
            for biz_attr in self._ordered_biz_attrs
        )

    def register(self, name, attr: BizAttribute):
        attr.name = name
        self._name_2_biz_attr[name] = attr
        self._group_map[attr.group][name] = attr
        bisect.insort(self._ordered_biz_attrs, attr)

    def by_name(self, name: Text) -> BizAttribute:
        return self._name_2_biz_attr.get(name, None)

    def by_group(self, group: Text) -> Dict[Text, BizAttribute]:
        return self._group_map[group]

    @property
    def relationships(self):
        return self.by_group(BizAttribute.PybizGroup.relationship)

    @property
    def views(self):
        return self.by_group(BizAttribute.PybizGroup.view)


class BizObjectMeta(type):

    builder = BizObjectTypeBuilder()

    def __new__(cls, name, bases, ns):
        if name != 'BizObject':
            ns = BizObjectMeta.builder.on_new(name, bases, ns)
        return type.__new__(cls, name, bases, ns)

    def __init__(biz_class, name, bases, ns):
        type.__init__(biz_class, name, bases, ns)
        if name != 'BizObject':
            BizObjectMeta.builder.on_init(name, biz_class)
            venusian.attach(
                biz_class, biz_class.venusian_callback, category='biz'
            )

    @staticmethod
    def venusian_callback(scanner, name, biz_class):
        console.info(f'venusian scan found "{biz_class.__name__}" BizObject')
        scanner.biz_classes.setdefault(name, biz_class)
