import bisect

from functools import reduce
from typing import List, Dict, Set, Text, Type, Tuple

from pybiz.util.misc_functions import is_bizobj, is_sequence

from ..biz_list import BizList


class QueryExecutor(object):
    def execute(self, query: 'Query'):
        biz_type = query.biz_type

        if query.params.where:
            if len(query.params.where) > 1:
                predicate = reduce(lambda x, y: x & y, query.params.where)
            else:
                predicate = query.params.where[0]
        else:
            predicate = (biz_type._id != None)

        records = biz_type.get_dao().query(
            predicate=predicate,
            fields=query.params.fields,
            order_by=query.params.order_by,
            limit=query.params.limit,
            offset=query.params.offset,
        )

        targets = biz_type.BizList(biz_type(x) for x in records)
        return self._execute_recursive(query, targets).clean()

    def _execute_recursive(self, query: 'Query', sources: List['BizObject']):
        # the class whose relationships we are executing:
        biz_type = query.biz_type

        # now sort attribute names by their BizAttribute priority.
        ordered_items = []
        for biz_attr_name, sub_query in query.params.attributes.items():
            biz_attr = biz_type.attributes.by_name(biz_attr_name)
            bisect.insort(ordered_items, (biz_attr, sub_query))

        # execute each BizAttribute on each BizObject individually. a nice to
        # have would be a bulk-execution interface built built into the
        # BizAttribute base class
        for biz_attr, sub_query in ordered_items:
            if biz_attr.category == 'relationship':
                relationship = biz_attr
                targets = relationship.execute(
                    sources,
                    select=sub_query.params.fields,
                    where=sub_query.params.where,
                    order_by=sub_query.params.order_by,
                    limit=sub_query.params.limit,
                    offset=sub_query.params.offset,
                )
                # execute nested relationships and then zip each
                # source BizObject up with its corresponding target
                # BizObjects, as returned by the BizAttribute.
                self._execute_recursive(sub_query, targets)
                for source, target in zip(sources, targets):
                    setattr(source, biz_attr.name, target)
            else:
                for source in sources:
                    if sub_query:
                        value = sub_query.execute(source)
                    else:
                        value = biz_attr.execute(source)
                    setattr(source, biz_attr.name, value)

        return sources
