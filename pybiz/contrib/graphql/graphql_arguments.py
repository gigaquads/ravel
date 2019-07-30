import re

from typing import Dict, Set, Text, List, Type, Tuple

from pybiz.biz.order_by import OrderBy

RE_ORDER_BY = re.compile(r'(\w+)\s+((?:desc)|(?:asc))', re.I)


class GraphQLArguments(object):

    @classmethod
    def parse(cls, biz_type, node, predicate_parser) -> 'GraphQLArguments':
        args = {
            arg.name: arg.value for arg in
            getattr(node, 'arguments', ())
        }
        return cls(
            where=cls._parse_where(
                biz_type, args.pop('where', None), predicate_parser
            ),
            order_by=cls._parse_order_by(args.pop('order_by', None)),
            offset=cls._parse_offset(args.pop('offset', None)),
            limit=cls._parse_limit(args.pop('limit', None)),
            custom=args
        )

    @classmethod
    def _parse_order_by(cls, order_by_strs: Tuple[Text]) -> Tuple[OrderBy]:
        order_by_strs = order_by_strs or []
        order_by_list = []
        for order_by_str in order_by_strs:
            match = RE_ORDER_BY.match(order_by_str)
            if match is not None:
                key, asc_or_desc = match.groups()
                order_by = OrderBy.load({
                    'desc': asc_or_desc.lower() == 'desc',
                    'key': key,
                })
                order_by_list.append(order_by)
        return tuple(order_by_list)

    @classmethod
    def _parse_offset(cls, raw_offset) -> int:
        offset = None
        if raw_offset is not None:
            offset = max(int(raw_offset), 0)
        return offset

    @classmethod
    def _parse_limit(cls, raw_limit) -> int:
        limit = None
        if raw_limit is not None:
            limit = max(int(raw_limit), 1)
        return limit

    @classmethod
    def _parse_where(
        cls,
        biz_type: Type['BizObject'],
        predicate_strings: List[Text],
        parser: 'PredicateParser',
    ) -> List['Predicate']:
        if isinstance(predicate_strings, str):
            predicate_strings = [predicate_strings]
        return [
            parser.parse(biz_type, pred_str)
            for pred_str in (predicate_strings or [])
        ]

    def __init__(self, where, order_by, offset, limit, custom):
        self.where = where
        self.order_by = order_by
        self.offset = offset
        self.limit = limit
        self.custom = custom

    def __getattr__(self, attr):
        return self.custom.get(attr)
