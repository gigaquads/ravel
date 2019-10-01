import copy
import inspect
import bisect
import venusian

from typing import Type, List, Dict, Tuple, Text
from collections import defaultdict

from appyratus.utils import DictObject

from pybiz.schema import Schema, fields, String, UuidString, Field, Int, Id
from pybiz.util.misc_functions import import_object, is_biz_obj
from pybiz.util.loggers import console
from pybiz.predicate import PredicateParser
from pybiz.constants import (
    IS_BIZ_OBJECT_ANNOTATION,
    ID_FIELD_NAME,
    REV_FIELD_NAME,
)

from ..field_property import FieldProperty
from ..biz_list import BizList, BizListClassBuilder
from ..biz_attribute import BizAttribute, BizAttributeProperty, BizAttributeManager
from ..biz_attribute.relationship import Relationship, RelationshipProperty
from ..biz_attribute.view import View, ViewProperty


class BizObjectClassBuilder(object):
    """
    The `BizObjectClassBuilder is used internally by the BizObjectMeta
    metaclass. The main reason it exists is to move logic out of the metaclass
    itself into  something else that is more testable and reads like normal OOP
    rather than metaclass magic.
    """
    def __init__(self):
        self._biz_list_class_builder = BizListClassBuilder()

    def prepare(self, name, bases, namespace):
        """
        Mutate the class attribute or "namespace" dict about to be used to
        build a new BizObject class via the metaclass __new__ method.
        """
        namespace.update({
            IS_BIZ_OBJECT_ANNOTATION: True,
            'pybiz': DictObject({
                'attributes': self._prepare_biz_attr_manager(bases, namespace),
                'is_abstract': self._prepare_is_abstract(namespace),
                'is_bootstrapped': False,

                # these are set in self.build:
                'schema': None,
                'field_defaults': None,
                'predicate_parser': None,
                'default_selectors': None,
                'all_selectors': None,
            })
        })

    def build(self, name, biz_class):
        """
        Build Pybiz class attributes for the given BizObject class. This is
        called by the metaclass __init__method.
        """
        # build cls.Schema
        self._build_schema_class(name, biz_class)

        # build property objects corresponding to each BizAttribute
        # picked up by the AttributeManager in cls.pybiz.attributes
        self._build_biz_attr_properties(biz_class)
        
        # build property objects corresponding to each Schema Field
        # declared on this new class or inherited from a base class.
        self._build_field_properties(biz_class)

        # create a singleton Schema instance for this biz_class
        biz_class.pybiz.schema = biz_class.Schema()

        # remove default values/callbacks set on schema Fields and store
        # them separately in this `field_defaults` dict. Otherwise, defaults
        # get generated in BizObject.__init__ when in fact we only want to
        # generate defaults during BizObject.create and create_many.
        biz_class.pybiz.field_defaults = self._extract_defaults(biz_class)

        # Create a predicate parser, which can be used to parse a string, like
        # "(name == 'Bob')" into its corresponding Pybiz Predicate object. This
        # is used, for instance, by GraphQL.
        biz_class.pybiz.predicate_parser = PredicateParser(biz_class)

        # `default_selectors` is filled in by the bootstrap logic of
        # Relationships set on this class. 
        biz_class.pybiz.default_selectors = {
            f.name for f in biz_class.Schema.fields.values()
            if (f.required or f.meta.get('pybiz_is_fk'))
        }

        # here is a set of all selectable field/attribute names on this
        # BizObject class.
        biz_class.pybiz.all_selectors = set(
            biz_class.Schema.fields.keys() |
            biz_class.pybiz.attributes.keys()
        )

        # build cls.BizList. This must happen after all_selectors is built in
        # order for the builder to know what bulk properties to build.
        self._biz_list_class_builder.build(biz_class)

        # these aliases exist for developer convenience:
        biz_class.schema = biz_class.pybiz.schema
        biz_class.attributes = biz_class.pybiz.attributes
        biz_class.relationships = biz_class.pybiz.attributes.relationships
        biz_class.views = biz_class.pybiz.attributes.views

        # this is for BizObject class autodiscovery:
        venusian.attach(biz_class, venusian_callback, category='biz')

    def _prepare_is_abstract(self, namespace):
        class_method = namespace.get('__abstract__')
        if class_method is not None:
            is_abstract = class_method.__func__
            return is_abstract()
        return False

    def _prepare_biz_attr_manager(self, bases: Tuple[Type], ns: Dict) -> Dict:
        manager = BizAttributeManager()
        for base in bases:
            if is_biz_obj(base):
                # inherit BizAttributes from base BizObject class
                for group, biz_attr in base.internal.attributes.items():
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

    def _build_field_properties(self, biz_class):
        """
        Here, we create all FieldProperties for each Field present in the
        BizObject's Schema. In addition, we collect all Id fields used
        internally by Pybiz. In overview, a FieldProperty is something like
        `User.email`, when accessed via the class object.
        """
        biz_class.pybiz.id_fields = set()
        for field in biz_class.Schema.fields.values():
            field_prop = FieldProperty(biz_class, field)
            setattr(biz_class, field.name, field_prop)
            if isinstance(field, Id):
                biz_class.pybiz.id_fields.add(field)

    def _build_biz_attr_properties(self, biz_class):
        """
        For each BizAttribute detected on the BizObject class, we create a
        corresponding BizAttributeProperty. The property class is determined by
        the `BizAttribute.build_property` method overriden on the BizAttribute
        subclass.
        """
        for biz_attr in biz_class.pybiz.attributes.values():
            biz_attr.bind(biz_class)
            biz_attr_prop = biz_attr.build_property()
            setattr(biz_class, biz_attr.name, biz_attr_prop)

    def _build_schema_class(self, name, biz_class):
        """
        Collect all Field objects declard on `biz_class` as well as those
        associated with any base BizObject class and dynamically construct a
        new Schema class with all of them. Then set it as the `Schema`
        attribute on biz_class.
        """
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

        # bless each biz_obj with mandatory built-in Fields
        if ID_FIELD_NAME not in fields:
            fields[ID_FIELD_NAME] = Id(nullable=True)
        if REV_FIELD_NAME not in fields:
            fields[REV_FIELD_NAME] = Int(nullable=True)

        fields.pop('schema', None)
        biz_class.Schema = Schema.factory(f'{name}Schema', fields)

    def _extract_defaults(self, biz_class: Type['BizObject']) -> Dict:
        """
        Pop all default values/callbacks declared on Schema fields into a
        separate dictionary for internal use when create or create_many is
        called.
        """
        # start with inherited defaults
        defaults = copy.deepcopy(getattr(biz_class, 'defaults', {}))

        # add any new defaults from the schema
        for k, field in biz_class.Schema.fields.items():
            if field.default is not None:
                defaults[k] = field.default
                field.default = None

        return defaults


class BizObjectMeta(type):
    """
    Metaclass used by all BizObject classes.
    """

    def __new__(cls, name, bases, namespace):
        """
        This is where Python creates the new BizObject subclass object itself.
        """
        cls.pybiz_builder = BizObjectClassBuilder()
        if name != 'BizObject':
            cls.pybiz_builder.prepare(name, bases, namespace)
        return type.__new__(cls, name, bases, namespace)

    def __init__(cls, name, bases, ns):
        """
        This is where Python initializes class attributes on the newly-created
        BizObject subclass.
        """
        type.__init__(cls, name, bases, ns)
        if name != 'BizObject':
            cls.pybiz_builder.build(name, cls)


def venusian_callback(scanner, name, biz_class):
    """
    Callback used by Venusian for BizObject class auto-discovery.
    """
    console.info(f'venusian scan found "{biz_class.__name__}" BizObject')
    scanner.biz_classes.setdefault(name, biz_class)
