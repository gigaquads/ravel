import sys
import copy
import inspect

import venusian

from abc import ABCMeta
from importlib import import_module

from appyratus.schema.fields import Field
from appyratus.schema import Schema

from pybiz.dao.base import DaoManager
from pybiz.web.patch import JsonPatchMixin
from pybiz.web.graphql import GraphQLEngine
from pybiz.constants import (
    IS_BIZOBJ_ANNOTATION,
    PRE_PATCH_ANNOTATION,
    POST_PATCH_ANNOTATION,
    PATCH_PATH_ANNOTATION,
    PATCH_ANNOTATION,
)

from .relationship import Relationship, RelationshipProperty
from .field_property import FieldProperty


class BizObjectMeta(ABCMeta):

    dao_manager = DaoManager()

    def __new__(cls, name, bases, dict_):
        new_class = ABCMeta.__new__(cls, name, bases, dict_)
        cls.add_is_bizobj_annotation(new_class)
        return new_class

    def __init__(cls, name, bases, dict_):
        ABCMeta.__init__(cls, name, bases, dict_)

        cls.graphql = GraphQLEngine(cls)
        cls.dao_manager = BizObjectMeta.dao_manager

        relationships = cls.build_relationships()
        schema_class = cls.build_schema_class(name)

        cls.build_all_properties(schema_class, relationships)
        cls.register_JsonPatch_hooks(bases)
        cls.register_dao()

        def venusian_callback(scanner, name, bizobj_type):
            scanner.bizobj_classes[name] = bizobj_type

        venusian.attach(cls, venusian_callback, category='biz')

    def register_dao(cls):
        manager = BizObjectMeta.dao_manager
        if not manager.is_registered(cls):
            dao_type = cls.__dao__()
            if dao_type:
                manager.register(cls, dao_type)

    def build_schema_class(cls, name):
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
                schema_class = cls.import_schema_class(obj)
            elif isinstance(obj, type) and issubclass(obj, Schema):
                schema_class = obj
            else:
                raise ValueError(str(obj))
        else:
            schema_class = None

        fields = copy.deepcopy(schema_class.fields) if schema_class else {}

        # "inherit" fields of parent BizObject.Schema
        inherited_schema_class = getattr(cls, 'Schema', None)
        if inherited_schema_class is not None:
            for k, v in inherited_schema_class.fields.items():
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

        # Build string name of the new Schema class
        # and construct the Schema class object:
        cls.Schema = Schema.factory('{}Schema'.format(name), fields)
        cls.schema = cls.Schema()

        return cls.Schema

    def add_is_bizobj_annotation(new_class):
        """
        Set this attribute in order to be able to use duck typing to check
        isinstance of BizObjects.
        """
        setattr(new_class, IS_BIZOBJ_ANNOTATION, True)

    def build_all_properties(cls, schema_class, relationships):
        """
        The names of Fields declared in the Schema and Relationships declared on
        the BizObject will overwrite any methods or attributes defined
        explicitly on the BizObject class. This happens here.
        """
        cls.build_relationship_properties(relationships)
        cls.relationships = relationships
        cls.build_field_properties(cls.schema, relationships)

    def import_schema_class(self, class_path_str):
        class_path = class_path_str.split('.')
        assert len(class_path) > 1

        module_path_str = '.'.join(class_path[:-1])
        class_name = class_path[-1]

        try:
            schema_module = import_module(module_path_str)
            schema_class = getattr(schema_module, class_name)
        except Exception:
            raise ImportError(
                'failed to import schema class {}'.format(class_name)
            )

        return schema_class

    def register_JsonPatch_hooks(cls, bases):
        if not any(issubclass(x, JsonPatchMixin) for x in bases):
            return

        # scan class methods for those annotated as patch hooks
        # and register them as such.
        for k, v in inspect.getmembers(cls, predicate=inspect.ismethod):
            path = getattr(v, PATCH_PATH_ANNOTATION, None)
            if hasattr(v, PRE_PATCH_ANNOTATION):
                assert path
                cls.add_pre_patch_hook(path, k)
            elif hasattr(v, PATCH_ANNOTATION):
                assert path
                cls.set_patch_hook(path, k)
            elif hasattr(v, POST_PATCH_ANNOTATION):
                assert path
                cls.add_post_patch_hook(path, k)

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
