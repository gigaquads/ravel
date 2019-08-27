import bisect

from functools import reduce
from typing import List, Dict, Set, Text, Type, Tuple

from pybiz.util.misc_functions import is_bizobj, is_sequence

from ..biz_list import BizList


class QueryExecutor(object):
    def execute(
        self,
        query: 'Query',
        backfiller: 'QueryBackfiller' = None,
        constraints: Dict[Text, 'Constraint'] = None,
        first: bool = False,
        fetch: bool = True,
    ):
        biz_class = query.biz_class

        if query.params.where:
            if len(query.params.where) > 1:
                predicate = reduce(lambda x, y: x & y, query.params.where)
            else:
                predicate = query.params.where[0]
        else:
            predicate = (biz_class._id != None)

        if fetch:
            records = biz_class.get_dao().query(
                predicate=predicate,
                fields=query.params.fields,
                order_by=query.params.order_by,
                limit=query.params.limit,
                offset=query.params.offset,
            )
            targets = biz_class.BizList(
                biz_class(x) for x in records
            ).clean()
        else:
            targets = biz_class.BizList()

        if (not targets) and backfiller is not None:
            targets = backfiller.generate(
                query=query,
                count=(1 if first else None),
                constraints=constraints,
            )

        return self._execute_recursive(
            query, backfiller, targets, fetch
        )

    def _execute_recursive(
        self,
        query: 'Query',
        backfiller: 'QueryBackfiller',
        sources: List['BizObject'],
        fetch: bool,
    ):
        # the class whose relationships we are executing:
        source_biz_class = query.biz_class

        # now sort attribute names by their BizAttribute priority.
        ordered_items = []
        for biz_attr_name, subquery in query.params.attributes.items():
            biz_attr = source_biz_class.attributes.by_name(biz_attr_name)
            bisect.insort(ordered_items, (biz_attr, subquery))

        # execute each BizAttribute on each BizObject individually. a nice to
        # have would be a bulk-execution interface built built into the
        # BizAttribute base class
        for biz_attr, subquery in ordered_items:
            if biz_attr.category == 'relationship':
                relationship = biz_attr
                limit = subquery.params.limit
                params = {
                    'select': set(subquery.params.fields.keys()),
                    'where': subquery.params.where,
                    'order_by': subquery.params.order_by,
                    'limit': subquery.params.limit,
                    'offset': subquery.params.offset or 0,
                }
                targets = relationship.execute(sources, **params)
                # execute nested relationships and then zip each
                # source BizObject up with its corresponding target
                # BizObjects, as returned by the BizAttribute.
                for source, target in zip(sources, targets):
                    if (not target) and backfiller is not None:
                        target = relationship.generate(
                            source, backfiller=backfiller, **params
                        )
                    elif (limit is not None) and (len(target) < limit):
                        params['limit'] = limit - len(target)
                        target.extend(relationship.generate(
                            source, backfiller=backfiller, fetch=False, **params
                        ))
                    setattr(source, biz_attr.name, target)
                self._execute_recursive(subquery, backfiller, targets, fetch)
            else:
                for source in sources:
                    if subquery:
                        value = subquery.execute(source)
                    else:
                        value = biz_attr.execute(source)
                    setattr(source, biz_attr.name, value)

        return sources
