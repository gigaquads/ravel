import bisect

from functools import reduce
from typing import List, Dict, Set, Text, Type, Tuple

from pybiz.util.misc_functions import is_bizobj, is_bizlist, is_sequence

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

        self._execute_recursive(query, backfiller, targets, fetch)
        return targets

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
        ordered_biz_attrs = self._sort_biz_attrs(query)

        # execute each BizAttribute on each BizObject individually. a nice to
        # have would be a bulk-execution interface built built into the
        # BizAttribute base class
        for biz_attr in ordered_biz_attrs:
            sub_query = query.params.attributes[biz_attr.name]

            # process Relationships...
            if biz_attr.category == 'relationship':
                rel = biz_attr
                params = self._prepare_relationship_query_params(sub_query)

                # execute the relationship's underlying query before
                # backfilling if necessary.
                target_biz_things = rel.execute(sources, **params)

                # this accumulator is used to collect ALL "target" BizObjects
                # for the sake of batching and passing into the next recursive
                # method call:
                target_biz_objects_acc = set()

                # Zip up each source BizObject with corresponding targets
                for source, target_biz_thing in zip(sources, target_biz_things):
                    if backfiller is not None:
                        target_biz_thing = self._backfill_relationship(
                            source, target_biz_thing, rel, backfiller, params
                        )
                    if rel.many:
                        target_biz_objects_acc.update(target_biz_thing)
                    else:
                        target_biz_objects_acc.add(target_biz_thing)

                    setattr(source, rel.name, target_biz_thing)

                # recursively execute the subqueries of this subquery...
                self._execute_recursive(
                    query=sub_query,
                    backfiller=backfiller,
                    sources=rel.target_biz_class.BizList(target_biz_objects_acc),
                    fetch=fetch
                )
            else:
                # XXX: this needs debugging and reworking:
                for source in sources:
                    if sub_query:
                        value = sub_query.execute(source)
                    else:
                        value = biz_attr.execute(source)
                    setattr(source, biz_attr.name, value)

        return sources

    def _sort_biz_attrs(self, query):
        ordered_biz_attrs = []
        for biz_attr_name, sub_query in query.params.attributes.items():
            biz_attr = query.biz_class.attributes.by_name(biz_attr_name)
            bisect.insort(ordered_biz_attrs, biz_attr)
        return ordered_biz_attrs

    def _prepare_relationship_query_params(self, sub_query):
        """
        Translate the Query params data into the kwargs expected by
        Relationip.execute/generate.
        """
        return {
            'select': set(sub_query.params.fields.keys()),
            'where': sub_query.params.where,
            'order_by': sub_query.params.order_by,
            'limit': sub_query.params.limit,
            'offset': sub_query.params.offset,
        }

    def _backfill_relationship(
        self, source, target_biz_thing, rel, backfiller, params
    ):
        """
        Ensure that each target BizList is not empty and, if a limit is
        specified, has a length equal to said limit.
        """
        limit = params['limit']
        if limit is None:
            target_biz_thing = rel.generate(source, backfiller=backfiller, **params)
        elif limit and (len(target_biz_thing) < limit):
            assert is_bizlist(target_biz_thing)
            params['limit'] = limit - len(target_biz_thing)
            target_biz_thing.extend(
                rel.generate(
                    source, backfiller=backfiller, fetch=False, **params
                )
            )
        return target_biz_thing

