import copy

from typing import Dict, Text, List, Tuple, Set
from collections import defaultdict

from appyratus.utils import StringUtils, DictUtils

from pybiz.util import is_bizobj


class DumpSpecification(dict):
    def __init__(
        self,
        fields: Set[Text] = None,
        relationships: Dict[Text, 'DumpSpecification'] = None,
        limit: int = None,
        offset: int = None,
    ):
        self['fields'] = fields or []
        self['relationships'] = relationships or {}
        self['limit'] = max(1, limit) if limit is not None else None
        self['offset'] = max(0, offset) if offset is not None else None

    @property
    def fields(self):
        return self['fields']

    @fields.setter
    def fields(self, fields):
        self['fields'] = fields

    @property
    def relationships(self):
        return self['relationships']

    @relationships.setter
    def relationships(self, relationships):
        self['relationships'] = relationships

    @property
    def limit(self):
        return self['limit']

    @property
    def offset(self):
        return self['offset']


class DumpMethod(object):
    def dump(
        self,
        bizobj,
        depth: int,
        fields: Dict = None,
        parent: Dict = None,
    ):
        pass

    @staticmethod
    def normalize_fields(fields):
        if not fields:
            return None
        elif isinstance(fields, (list, tuple, set)):
            return DictUtils.unflatten_keys({k: True for k in fields})
        else:
            return DictUtils.unflatten_keys(fields)

    def dump_fields_backup(self, bizobj, include: Dict):
        # copy field data into the record
        record = {}
        for k, field in bizobj.schema.fields.items():
            if k == '_id':
                record['id'] = bizobj._id
            elif (include is not None) and (k not in include):
                continue
            elif not field.meta.get('private', False):
                v = bizobj.data.get(k)
                # convert data to primitive types recognized as valid JSON
                # and other serialization formats more generally
                if isinstance(v, (dict, list)):
                    record[k] = copy.deepcopy(v)
                elif isinstance(v, (set, tuple)):
                    record[k] = list(v)
                else:
                    record[k] = v

        return record

    def dump_fields(self, bizobj, spec: DumpSpecification):
        # copy field data into the record
        record = {}
        if not spec.fields:
            spec.fields = bizobj.schema.fields.keys()
        for k, field in bizobj.schema.fields.items():
            if k == '_id':
                record['id'] = bizobj._id
                if k not in spec.fields:
                    continue
            elif not field.meta.get('private', False):
                v = bizobj.data.get(k)
                # convert data to primitive types recognized as valid JSON
                # and other serialization formats more generally
                if isinstance(v, (dict, list)):
                    record[k] = copy.deepcopy(v)
                elif isinstance(v, (set, tuple)):
                    record[k] = list(v)
                else:
                    record[k] = v

        return record

    def insert_relationship_fields_backup(self, bizobj, record: Dict, include: Dict):
        for k, rel in bizobj.relationships.items():
            if (include is not None) and (k not in include):
                continue
            if rel.links:
                if callable(rel.links):
                    record[k] = rel.links(bizobj)
                else:
                    record[k] = getattr(bizobj, rel.links)

    def insert_relationship_fields(self, bizobj, record: Dict, spec: DumpSpecification):
        if not spec.relationships:
            spec.relationships = {
                k: True for k in bizobj.schema.fields.keys()
            }
        for k, rel in bizobj.relationships.items():
            if k not in spec.relationships:
                continue
            if rel.links:
                if callable(rel.links):
                    record[k] = rel.links(bizobj)
                else:
                    record[k] = getattr(bizobj, rel.links)


class NestingDumpMethod(DumpMethod):
    def dump(
        self,
        bizobj,
        depth: int,
        spec: Dict = None,
        parent: Dict = None,
    ):
        if spec is None:
            spec = DumpSpecification()
        elif isinstance(spec, dict):
            spec = DumpSpecification(**spec)

        record = self.dump_fields(bizobj, spec)

        if not depth:
            self.insert_relationship_fields(bizobj, record, spec)

        # recursively dump nested bizobjs
        for k, rel in bizobj.relationships.items():
            if k not in spec.relationships:
                continue

            child_spec = spec.relationships[k]
            if isinstance(child_spec, dict):
                child_spec = DumpSpecification(**child_spec)
            elif child_spec in (None, True):
                child_spec = DumpSpecification()

            if child_spec.fields:
                next_depth = min(1, depth - 1)
            elif depth > 0:
                next_depth = depth - 1
            else:
                continue

            # recursively expand relationships biz objects
            v = bizobj.related.get(k)
            if (v is None) and (rel.query is not None):
                v = rel.query(
                    bizobj,
                    fields=child_spec.fields,
                    limit=child_spec.limit,
                    offset=child_spec.offset,
                )

                if rel.many and not isinstance(v, (list, tuple, set)):
                    raise Exception('expected list')
                elif (not rel.many) and (not is_bizobj(v)):
                    raise Exception('expected BizObject instance')

            # dump the bizobj or list of bizobjs
            if is_bizobj(v):
                record[k] = self.dump(
                    v, next_depth, spec=child_spec, parent=record,
                )
            elif rel.many:
                record[k] = [
                    self.dump(
                        x, next_depth, spec=child_spec, parent=record,
                    ) for x in v
                ]
            else:
                record[k] = None

        return record


class SideLoadingDumpMethod(DumpMethod):
    def dump(
        self,
        bizobj,
        depth: int,
        fields: Dict = None,
        result: Dict = None
    ):
        if depth < 1:
            # base case
            result['links'] = dict(result['links'])
            return result

        if result is None:
            # if here, this is the initial call, not a recursive one
            include = self.normalize_fields(fields)
            record = self.dump_fields(bizobj, include)
            self.insert_relationship_fields(bizobj, record, include)
            result = {
                'target': record,
                'links': defaultdict(dict),
            }
        elif fields is True:
            include = None
        else:
            include = fields

        # recursively process child relationships
        for k, rel in bizobj.relationships.items():
            if (include is not None) and (k not in include):
                continue

            # get the related bizobj or list thereof
            obj = bizobj.related.get(k)

            # get the fields to query for the related bizobj(s)
            if include is not None:
                related_fields = include.get(k)
                if related_fields is True:
                    related_fields = None
            else:
                related_fields = None

            # lazy load the related bizobj(s)
            if (obj is None) and (rel.query is not None):
                obj = rel.query(bizobj, fields=related_fields)

            # put pairs of (bizobj, fields) into array for recursion
            if rel.many:
                related_items = zip(obj, [related_fields] * len(obj))
            else:
                related_items = [(obj, related_fields)]

            # recurse on child bizobjs
            for related_bizobj, related_fields in related_items:
                # "kind" is the name of the public resource type that appears in
                # the "links" result dict
                kind = StringUtils.snake(related_bizobj.__class__.__name__)
                related_id = related_bizobj._id

                # only bother adding to the links dict if not already done so by
                # another bizobj higher in the tree.
                if related_id not in result['links'][kind]:
                    related_record = self.dump_fields(related_bizobj, related_fields)
                    self.insert_relationship_fields(
                        related_bizobj, related_record, related_fields
                    )
                    result['links'][kind][related_id] = related_record
                    self.dump(
                        related_bizobj,
                        depth-1,
                        fields=related_fields,
                        result=result
                    )

        return result


