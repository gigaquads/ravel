import bisect

from functools import reduce
from typing import List, Dict, Set, Text, Type, Tuple

from pybiz.util.misc_functions import is_bizobj, is_sequence

from ..biz_list import BizList


class QueryExecutor(object):
    def execute(
        self,
        query: 'Query',
        backfiller: 'Backfiller' = None,
        constraints: Dict[Text, 'Constraint'] = None,
        first: bool = False,
    ):
        biz_class = query.biz_class

        if query.params.where:
            if len(query.params.where) > 1:
                predicate = reduce(lambda x, y: x & y, query.params.where)
            else:
                predicate = query.params.where[0]
        else:
            predicate = (biz_class._id != None)

        records = biz_class.get_dao().query(
            predicate=predicate,
            fields=query.params.fields,
            order_by=query.params.order_by,
            limit=query.params.limit,
            offset=query.params.offset,
        )

        targets = biz_class.BizList(biz_class(x) for x in records)
        if (not targets) and backfiller is not None:
            targets = backfiller.generate(
                query=query,
                count=(1 if first else None),
                constraints=constraints,
            )

        return self._execute_recursive(query, backfiller, targets)

    def _execute_recursive(
        self,
        query: 'Query',
        backfiller: 'Backfiller',
        sources: List['BizObject'],
    ):
        # the class whose relationships we are executing:
        biz_class = query.biz_class

        # now sort attribute names by their BizAttribute priority.
        ordered_items = []
        for biz_attr_name, subquery in query.params.attributes.items():
            biz_attr = biz_class.attributes.by_name(biz_attr_name)
            bisect.insort(ordered_items, (biz_attr, subquery))

        # execute each BizAttribute on each BizObject individually. a nice to
        # have would be a bulk-execution interface built built into the
        # BizAttribute base class
        for biz_attr, subquery in ordered_items:
            if biz_attr.category == 'relationship':
                relationship = biz_attr
                targets = relationship.execute(
                    sources,
                    select=set(subquery.params.fields.keys()),
                    where=subquery.params.where,
                    order_by=subquery.params.order_by,
                    limit=subquery.params.limit,
                    offset=subquery.params.offset,
                )
                # execute nested relationships and then zip each
                # source BizObject up with its corresponding target
                # BizObjects, as returned by the BizAttribute.
                self._execute_recursive(subquery, backfiller, targets)
                for source, target in zip(sources, targets):
                    if (not target) and backfiller is not None:
                        target = relationship.generate(
                            source,
                            select=set(subquery.params.fields.keys()),
                            where=subquery.params.where,
                            order_by=subquery.params.order_by,
                            limit=subquery.params.limit,
                            offset=subquery.params.offset,
                            backfiller=backfiller,
                        )
                    setattr(source, biz_attr.name, target)
            else:
                for source in sources:
                    if subquery:
                        value = subquery.execute(source)
                    else:
                        value = biz_attr.execute(source)
                    setattr(source, biz_attr.name, value)

        return sources
