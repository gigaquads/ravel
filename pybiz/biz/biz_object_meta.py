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
from pybiz.schema import Schema, fields, String, Field, Int
from pybiz.util import import_object, is_bizobj

from .biz_list import BizList
from .relationship import Relationship
from .internal.relationship_property import RelationshipProperty
from .internal.field_property import FieldProperty


class BizObjectMeta(ABCMeta):
    def __new__(cls, name, bases, dict_):
        # prepare the relationships dict
        relationships = {}
        for k, v in list(dict_.items()):
            if isinstance(v, Relationship):
                relationships[k] = v
                del dict_[k]
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

        # inject relationships dict into class namespace
        dict_['relationships'] = relationships
        dict_['is_bootstrapped'] = False
        dict_['is_abstract'] = dict_.get('is_abstract', False)
        dict_[IS_BIZOBJ_ANNOTATION] = True

        # now make the new class with the modified dict_
        new_class = ABCMeta.__new__(cls, name, bases, dict_)
        return new_class

    def __init__(cls, name, bases, dict_):
        ABCMeta.__init__(cls, name, bases, dict_)

        schema_type = cls.build_schema_type(name)

        cls.schema = schema_type()
        cls.bind_relationships()
        cls.build_relationship_properties(cls.relationships)
        cls.build_field_properties(cls.schema, cls.relationships)

        cls.Schema = schema_type
        cls.BizList = BizList.type_factory(cls)

        def venusian_callback(scanner, name, biz_type):
            scanner.biz_types.setdefault(name, biz_type)

        venusian.attach(cls, venusian_callback, category='biz')

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
            fields['_id'] = String(nullable=True)
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

    def bind_relationships(cls):
        for k, rel in cls.relationships.items():
            rel.associate(cls, k)

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
