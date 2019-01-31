from typing import List, Dict, Set, Text, Type, Tuple

from appyratus.utils import DictUtils

from pybiz.util import is_bizobj
from pybiz.constants import IS_BIZOBJ_ANNOTATION


class QuerySpecification(tuple):
    """
    A `QuerySpecification` is a named tuple, containing a specification of which
    fields we are selecting from a target `BizObject`, along with the fields
    nested inside related instance objects, declared in a `Relationship`.
    """

    # correspondance between tuple field names
    # to tuple positional indexes:
    name2index = {
        'fields': 0,
        'relationships': 1,
        'limit': 2,
        'offset': 3,
        'order_by': 4,
    }

    def __new__(
        cls,
        fields: Set[Text] = None,
        relationships: Dict[Text, 'QuerySpecification'] = None,
        limit: int  =None,
        offset: int = None,
        order_by: Tuple = None,
    ):
        # set epxected default values for items in the tuple.
        # always work on a copy of the input `fields` set.
        fields = set(fields or set())

        if relationships is None:
            relationships = {}

        if limit is not None:
            limit = min(1, limit)

        if offset is not None:
            offset = max(0, offset)

        order_by = tuple(order_by or tuple())

        return tuple.__new__(cls, (
            fields,
            relationships,
            limit,
            offset,
            order_by,
        ))

    def __getattr__(self, name):
        """
        Access tuple element by name.
        """
        return self[self.name2index[name]]

    @staticmethod
    def prepare(
        spec: 'QuerySpecification',
        bizobj_type: Type['BizObject']
    ) -> 'QuerySpecification':
        """
        Translate input "spec" data structure into a well-formed
        QuerySpecification object with appropriate starting conditions.
        """
        def recursive_init(bizobj_type, names):
            spec = QuerySpecification()
            if '*' in names:
                spec.fields = set(bizobj_type.schema.fields.keys())
                del names['*']
            for k, v in names.items():
                if k in bizobj_type.schema.fields:
                    spec.fields.add(k)
                elif k in bizobj_type.relationships:
                    rel = bizobj_type.relationships[k]
                    if v is None:  # => is terminal
                        spec.relationships[k] = QuerySpecification()
                    elif isinstance(v, dict):
                        spec.relationships[k] = recursive_init(rel.target, v)
            return spec

        if isinstance(spec, QuerySpecification):
            if '*' in spec.fields:
                 spec.fields = set(bizobj_type.schema.fields.keys())
        elif isinstance(spec, dict):
            names = DictUtils.unflatten_keys({k: None for k in spec})
            spec = recursive_init(bizobj_type, names)
        elif isinstance(spec, (set, list, tuple)):
            # spec is an array of field and relationship names
            # so partition the names between fields and relationships
            # in a new spec object.
            names = DictUtils.unflatten_keys({k: None for k in spec})
            spec = recursive_init(bizobj_type, names)
        elif spec is None:
            # by default, a new spec includes all fields and relationships
            spec = QuerySpecification(
                fields={k for k, field in bizobj_type.schema.fields.items()},
            )

        # ensure that _id and required fields are *always* specified
        spec.fields |= bizobj_type.schema.required_fields.keys()
        spec.fields.add('_id')

        return spec


class Query(object):
    def __init__(
        self,
        bizobj_type: Type['BizObject'],
        predicate: 'Predicate',
        spec: 'QuerySpecification'
    ):
        """
        Execute a recursive query according to a given logical match predicate
        and target field/relationship spec.
        """
        self.bizobj_type = bizobj_type
        self.dao = bizobj_type.get_dao()
        self.spec = QuerySpecification.prepare(spec, bizobj_type)
        self.predicate = predicate

    def execute(self) -> List['BizObject']:
        """
        Recursively query fields from the target `BizObject` along with all
        fields nested inside related objects declared in with `Relationship`.
        """
        records = self.dao.query(
            predicate=self.predicate,
            fields=self.spec.fields,
            limit=self.spec.limit,
            offset=self.spec.offset,
            order_by=self.spec.order_by,
        )
        multiset = self.bizobj_type.Multiset([
            self._recursive_execute(
                bizobj=self.bizobj_type(record).clean(),
                spec=self.spec
            ).clean()
            for record in records
        ])
        return multiset

    def _recursive_execute(
        self,
        bizobj: 'BizObject',
        spec: 'QuerySpecification'
    ) -> 'BizObject':
        """
        Recurse through all of the target biz object's relationships.
        """
        if bizobj is None:
            return None
        for k, child_spec in spec.relationships.items():
            rel = bizobj.relationships[k]
            v = rel.query(bizobj, child_spec)
            setattr(bizobj, k, v)
            if rel.many:
                for child in v:
                    self._recursive_execute(child, child_spec)
            else:
                self._recursive_execute(v, child_spec)
        return bizobj


class QueryUtils(object):
    """
    Misc functions used by BizObject to implement the get and get_many methods.
    """

    @classmethod
    def prepare_fields_argument(
        cls,
        bizobj_type: Type['BizObject'],
        argument: object,
        parent: Type['BizObject'] = None,
    ) -> Tuple[Set[Text], Dict[Text, Dict]]:
        """
        Normalize the `fields` argument to BizObject.get and get_many.
        """
        # standardized the `argument` to a nested dict structure
        if argument is None:
            # if none, specified, select all fields and relationships
            spec = {
                k: None for k in bizobj_type.schema.fields.keys()
            }
        else:
            dict_keys_type = type({}.keys())
            if isinstance(argument, (dict_keys_type, set, list, tuple)):
                spec = DictUtils.unflatten_keys({k: None for k in argument})
            elif isinstance(argument, dict):
                if parent is None:
                    spec = DictUtils.unflatten_keys(argument)
                else:
                    spec = argument
            else:
                spec = {}

            for k in bizobj_type.schema.fields:
                spec.setdefault(k, None)

        if '*' in spec:
            del spec['*']
            spec.update({k: None for k in bizobj_type.schema.fields})

        fields = set()      # <- set of fields to query on this bizobj_type
        relationships = {}  # <- map from relationship name to recursive result

        # recursively partition keys between the `fields`
        # set and `relationships` dict
        for k, v in spec.items():
            if k in bizobj_type.schema.fields:
                fields.add(k)
            elif k in bizobj_type.relationships:
                rel = bizobj_type.relationships[k]
                if v is None:
                    related_fields = {}
                else:
                    related_fields = v

                # recurse on relationship
                relationships[k] = cls.prepare_fields_argument(
                    bizobj_type=rel.target,
                    argument=related_fields,
                    parent=bizobj_type,
                )

        return (fields, relationships)

    @classmethod
    def query_relationships(cls, bizobj, children):
        """
        Works in concert with prepare_fields_argument. See get or get_many in
        BizObject.
        """
        if bizobj is not None:
            for k, (related_fields, nested_children) in children.items():
                rel = bizobj.relationships[k]
                related = rel.query(bizobj, related_fields)
                setattr(bizobj, k, related)
                if not related:
                    continue
                if is_bizobj(related):
                    cls.query_relationships(related, nested_children)
                else:
                    for x in related:
                        cls.query_relationships(x, nested_children)


class QueryResult(list):

    class Reducer(object):
        def __init__(self, query_result):
            self.query_result = query_result

        def __getattr__(self, key):
            if key not in self.query_result.bizobj_type.schema.fields:
                raise AttributeError(f'unrecognized field name: {key}')
            return [i[key] for i in self.query_result]

    def __init__(self, bizobj_type, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bizobj_type = bizobj_type
        self.f = QueryResult.Reducer(self)

    def dump(self, *args, **kwargs):
        return [
            bizobj.dump(*args, **kwargs)
            for bizobj in self
        ]
