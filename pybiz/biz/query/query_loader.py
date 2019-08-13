import pybiz.biz

from typing import Set, Text, Type

from appyratus.utils import DictUtils

from pybiz.schema import Schema, fields
from pybiz.predicate import Predicate


class QueryLoader(object):

    class Schema(Schema):
        alias = fields.String()
        limit = fields.Int(nullable=True)
        offset = fields.Int(nullable=True)
        order_by = fields.List(fields.String(), default=[])
        where = fields.List(fields.Dict(), nullable=True)
        target = fields.Nested({
            'type': fields.String(),
            'attributes': fields.Dict(default={}),
            'fields': fields.Dict(default={}),
        })

    def __init__(self):
        self._schema = QueryLoader.Schema()

    def load(self, biz_type, data):
        data, errors = self._schema.process(data)
        if errors:
            # TODO: raise custom exceptions
            raise ValueError(str(errors))

        sub_queries = []
        for v in data['sub_queries'].values():
            child_biz_type_name = v['target']['type']
            child_biz_type = biz_type.app.biz[child_biz_type_name]
            sub_queries.append(self.load(child_biz_type, v))

        targets = sub_queries.copy()
        targets += list(data['target']['fields'].keys())
        targets += list(data['target']['attributes'].keys())

        order_by = [OrderBy.load(x) for x in data['order_by']]

        query = pybiz.biz.Query(biz_type=biz_type, alias=data['alias'])
        query.select(targets)
        query.order_by(order_by)
        query.limit(data['limit'])
        query.offset(data['offset'])

        if data['where']:
            query.where([
                Predicate.load(biz_type, x) for x in data['where']
            ])

        return query

    @classmethod
    def from_keys(
        cls, biz_type: Type['BizObject'], keys: Set[Text]=None, tree=None
    ) -> 'Query':
        """
        Create a Query from a list of dotted field paths.
        """
        query = pybiz.biz.Query(biz_type)

        if tree is None:
            assert keys
            tree = DictUtils.unflatten_keys({k: None for k in keys})

        if '*' in tree:
            del tree['*']
            tree.update({
                k: None for k, v in biz_type.schema.fields.items()
                if not v.meta.get('private', False)
            })
        elif not tree:
            tree = {'_id': None, '_rev': None}

        for k, v in tree.items():
            if isinstance(v, dict):
                rel = biz_type.relationships[k]
                sub_query = cls.from_keys(rel.target_biz_type, tree=v)
                sub_query.alias = rel.name
                query._add_target(sub_query, None)
            else:
                query._add_target(k, v)

        return query
