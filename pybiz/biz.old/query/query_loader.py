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

    def load(self, biz_class, data):
        data, errors = self._schema.process(data)
        if errors:
            # TODO: raise custom exceptions
            raise ValueError(str(errors))

        sub_queries = []
        for v in data['sub_queries'].values():
            child_biz_class_name = v['target']['type']
            child_biz_class = biz_class.app.biz[child_biz_class_name]
            sub_queries.append(self.load(child_biz_class, v))

        targets = sub_queries.copy()
        targets += list(data['target']['fields'].keys())
        targets += list(data['target']['attributes'].keys())

        order_by = [OrderBy.load(x) for x in data['order_by']]

        query = pybiz.biz.Query(biz_class=biz_class, alias=data['alias'])
        query.select(targets)
        query.order_by(order_by)
        query.limit(data['limit'])
        query.offset(data['offset'])

        if data['where']:
            query.where([
                Predicate.load(biz_class, x) for x in data['where']
            ])

        return query
