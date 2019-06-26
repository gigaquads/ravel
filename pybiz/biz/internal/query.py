from typing import List, Dict, Set, Text, Type, Tuple

from appyratus.utils import DictUtils

from pybiz.util import is_bizobj, is_sequence
from pybiz.constants import IS_BIZOBJ_ANNOTATION

from ..biz_list import BizList


class QuerySpecification(object):
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

    def __init__(
        self,
        fields: Set[Text] = None,
        relationships: Dict[Text, 'QuerySpecification'] = None,
        limit: int = None,
        offset: int = None,
        order_by: Tuple = None,
        kwargs: Dict = None,
    ):
        # set epxected default values for items in the tuple.
        # always work on a copy of the input `fields` set.
        self.fields = set(fields) if fields else set()
        self.relationships = {} if relationships else {}
        self.limit = min(1, limit) if limit is not None else None
        self.offset = max(0, offset) if offset is not None else None
        self.order_by = tuple(order_by) if order_by else tuple()
        self.kwargs = kwargs or {}

        self._tuplized_attrs = (
            self.fields,
            self.relationships,
            self.limit,
            self.offset,
            self.order_by,
            self.kwargs,
        )

    def __getitem__(self, index):
        """
        Access tuple element by name.
        """
        return self._tuplized_attrs[index]

    def __len__(self):
        return len(self._tuplized_attrs)

    def __iter__(self):
        return iter(self._tuplized_attrs)

    @classmethod
    def build(
        cls, spec, biz_type: Type['BizObject'], fields=None,
    ) -> 'QuerySpecification':
        """
        Translate input "spec" data structure into a well-formed
        QuerySpecification with appropriate starting conditions.
        """
        fields = fields or {}

        def build_recursive(biz_type: Type['BizObject'], names: Dict):
            spec = cls()
            if '*' in names:
                del names['*']
                spec.fields = set(
                    f.source for f in biz_type.schema.fields.values()
                )
            for k, v in names.items():
                field = biz_type.schema.fields.get(k)
                if field:
                    spec.fields.add(field.source)
                elif k in biz_type.relationships:
                    rel = biz_type.relationships[k]
                    if v is None:  # base case
                        spec.relationships[k] = cls()
                    elif isinstance(v, dict):
                        spec.relationships[k] = build_recursive(rel.target, v)
            return spec

        if isinstance(spec, cls):
            if '*' in spec.fields:
                spec.fields = set(biz_type.schema.fields.keys())
        elif isinstance(spec, dict):
            names = DictUtils.unflatten_keys({k: None for k in spec})
            spec = build_recursive(biz_type, names)
        elif is_sequence(spec):
            # spec is an array of field and relationship names
            # so partition the names between fields and relationships
            # in a new spec object.
            names = DictUtils.unflatten_keys({k: None for k in spec})
            spec = build_recursive(biz_type, names)
        elif not spec:
            # by default, a new spec includes all fields and relationships
            spec = cls(
                fields={f.source for f in biz_type.schema.fields.values()},
            )

        # ensure that _id and required fields are *always* specified
        spec.fields |= {
            f.source for f in biz_type.schema.required_fields.values()
        }
        spec.fields.add(biz_type.schema.fields['_id'].source)

        if fields:
            tmp_spec = build_recursive(
                biz_type, DictUtils.unflatten_keys({k: None for k in fields})
            )
            spec.fields.update(tmp_spec.fields)
            spec.relationships.update(tmp_spec.relationships)

        return spec


class Query(object):
    def __init__(
        self,
        biz_type: Type['BizObject'],
        predicate: 'Predicate',
        spec: 'QuerySpecification',
        fields: Set[Text] = None,
    ):
        """
        Execute a recursive query according to a given logical match predicate
        and target field/relationship spec.
        """
        self.biz_type = biz_type
        self.dao = biz_type.get_dao()
        self.spec = QuerySpecification.build(spec, biz_type, fields=fields)
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
        return [
            self._recursive_execute(
                bizobj=self.biz_type(record).clean(),
                spec=self.spec
            ).clean()
            for record in records
        ]

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
            v = rel.query(
                bizobj,
                fields=child_spec.fields,
                limit=child_spec.limit,
                offset=child_spec.offset,
                ordering=child_spec.ordering,
                kwargs=child_spec.kwargs,
            )
            setattr(bizobj, k, v)
            if rel.many:
                assert isinstance(v, BizList)
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
        biz_type: Type['BizObject'],
        argument: object,
        depth=0,
        parent: Type['BizObject'] = None,
    ) -> Tuple[Set[Text], Dict[Text, Dict]]:
        """
        Normalize the `fields` argument to BizObject.get and get_many.
        """
        # standardized the `argument` to a nested dict structure
        if argument is None:
            # if none, specified, select all fields and relationships
            spec = {f.name: None for f in biz_type.schema.fields.values()}
        else:
            if is_sequence(argument):
                spec = DictUtils.unflatten_keys({k: None for k in argument})
            elif isinstance(argument, dict):
                if parent is None:
                    spec = DictUtils.unflatten_keys(argument)
                else:
                    spec = argument
            if '*' in spec:
                del spec['*']
                spec.update({f.name: None for f in biz_type.schema.fields.values()})
            if not spec:
                spec = {
                    f.name: None for f.name in
                    biz_type.schema.required_fields.values()
                }

        fields = set()      # <- set of fields to query on this biz_type
        relationships = {}  # <- map from relationship name to recursive result

        if depth > 0:
            spec.update({
                k: set(biz_type.relationships[k].biz_type.schema.fields.keys())
                for k in (biz_type.relationships.keys() - spec.keys())
            })

        # recursively partition keys between the `fields`
        # set and `relationships` dict
        for k, v in spec.items():
            if k in biz_type.schema.fields:
                field = biz_type.schema.fields[k]
                fields.add(field.source)
            elif k in biz_type.relationships:
                rel = biz_type.relationships[k]
                if v is None:
                    related_fields = {}
                else:
                    related_fields = v

                # recurse on relationship
                relationships[k] = cls.prepare_fields_argument(
                    biz_type=rel.target,
                    argument=related_fields,
                    depth=depth-1,
                    parent=biz_type,
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
                related = rel.query(bizobj, fields=related_fields)
                rel.set_internally(bizobj, related)
                if not related:
                    continue
                if is_bizobj(related):
                    cls.query_relationships(related, nested_children)
                else:
                    for x in related:
                        cls.query_relationships(x, nested_children)
