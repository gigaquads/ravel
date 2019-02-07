import sys
import copy
import inspect
import threading

import venusian

from abc import ABCMeta
from typing import Type, List

from pybiz.dao.base import Dao
from pybiz.dao.dao_binder import DaoBinder
from pybiz.dao.python_dao import PythonDao
from pybiz.constants import IS_BIZOBJ_ANNOTATION
from pybiz.schema import Schema, fields, Field, Int
from pybiz.util import import_object

from .relationship import Relationship
from .relationship_property import RelationshipProperty
from .field_property import FieldProperty
from .biz_list import BizList


class BizObjectMeta(ABCMeta):
    def __new__(cls, name, bases, dict_):
        new_class = ABCMeta.__new__(cls, name, bases, dict_)
        cls.add_is_bizobj_annotation(new_class)
        return new_class

    def __init__(cls, name, bases, dict_):
        ABCMeta.__init__(cls, name, bases, dict_)

        cls.Schema = cls.build_schema_type(name)

        cls.schema = cls.Schema()
        cls.relationships = cls.build_relationships()
        cls.build_relationship_properties(cls.relationships)
        cls.build_field_properties(cls.schema, cls.relationships)
        cls.register_dao()

        cls.BizList = BizList.type_factory(cls)

        def venusian_callback(scanner, name, biz_type):
            scanner.biz_types.setdefault(name, biz_type)

        venusian.attach(cls, venusian_callback, category='biz')

    def register_dao(cls):
        binder = DaoBinder.get_instance()

        dao_type_or_instance = cls.__dao__()

        if isinstance(dao_type_or_instance, type):
            dao_instance = dao_type_or_instance()
        elif isinstance(dao_type_or_instance, Dao):
            dao_instance = dao_type_or_instance
        else:
            # default to PythonDao
            dao_instance = PythonDao()

        # TODO: Insert Dao class into manifest.types.dao somehow if DNE
        binder.register(biz_type=cls, dao_instance=dao_instance)

    def build_schema_type(cls, name):
        """
        Builds cls.Schema from the fields declared on the business object. All
        business objects automatically inherit an _id field.
        """

        # We begin by building the `fields` dict that will become attributes
        # of our dynamic Schema class being created below.
        # Ensure each BizObject Schema class has an _id Field

        # use the schema class override if defined
        obj = cls.__schema__()
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
        inherited_schema_type = getattr(cls, 'Schema', None)
        if inherited_schema_type is not None:
            for k, v in inherited_schema_type.fields.items():
                fields.setdefault(k, copy.deepcopy(v))

        # collect and field declared on this BizObject class
        for k, v in inspect.getmembers(
            cls, predicate=lambda x: isinstance(x, Field)
        ):
            if k is 'schema':
                # XXX `schema` gets recognized by getmembers as a Field
                continue
            fields[k] = v

        # bless each bizobj with a mandatory _id field.
        if '_id' not in fields:
            fields['_id'] = Field(nullable=True)
        if '_rev' not in fields:
            fields['_rev'] = Int(nullable=True)

        # Normally, the Field `default` kwarg is generated upon Field.process
        # but we don't want this. We only want to apply the default upon
        # BizObject.save. Therefore, we unset the `default` attribute on all
        # fields and take care of setting defaults in custom BizObject logic.
        inherited_defaults = getattr(cls, 'defaults', {})
        cls.defaults = copy.deepcopy(inherited_defaults)

        # collect remaining defaults
        for k, field in fields.items():
            if (k not in cls.defaults) and (field.default is not None):
                cls.defaults[k] = field.default
                field.default = None

        # Build string name of the new Schema class
        # and construct the Schema class object:
        return Schema.factory('{}Schema'.format(name), fields)

    def add_is_bizobj_annotation(new_class):
        """
        Set this attribute in order to be able to use duck typing to check
        isinstance of BizObjects.
        """
        setattr(new_class, IS_BIZOBJ_ANNOTATION, True)

    def build_relationships(cls):
        # aggregate all relationships delcared on the bizobj
        # class into a single "relationships" dict.
        direct_relationships = {}
        inherited_relationships = {}

        is_not_method = lambda x: not inspect.ismethod(x)
        for k, rel in inspect.getmembers(cls, predicate=is_not_method):
            is_relationship = isinstance(rel, Relationship)
            if not is_relationship:
                is_super_relationship = k in cls.relationships
                if is_super_relationship:
                    super_rel = cls.relationships[k]
                    rel = copy.deepcopy(super_rel)
                    rel.bind(cls, k)
                    inherited_relationships[k] = rel
            else:
                direct_relationships[k] = rel
                rel.bind(cls, k)

        # clear the Relationships delcared on this subclass
        # from the class name space, to be replaced dynamically
        # with properties later on.
        for k in direct_relationships:
            delattr(cls, k)

        inherited_relationships.update(direct_relationships)
        return inherited_relationships

    def build_field_properties(cls, schema, relationships):
        """
        Create properties out of the fields declared on the schema associated
        with the class.
        """

        def build_property(k):
            def fget(self):
                if (k not in self.data) and '_id' in self._data:
                    # try to lazy load the field value
                    field = self.schema.fields.get(k)
                    if field and field.meta.get('lazy', True):
                        record = self.dao.fetch(_id=self._data['_id'], fields={k})
                        self._data[k] = record[k]
                return self[k]

            def fset(self, value):
                self[k] = value

            def fdel(self):
                del self[k]

            return FieldProperty(k, fget=fget, fset=fset, fdel=fdel)

        for field_name, field in schema.fields.items():
            if field_name not in relationships:
                field_prop = FieldProperty.build(cls, field)
                setattr(cls, field_name, field_prop)

    def build_relationship_properties(cls, relationships):
        for rel in relationships.values():
            rel_prop = RelationshipProperty.build(rel)
            setattr(cls, rel.name, rel_prop)
