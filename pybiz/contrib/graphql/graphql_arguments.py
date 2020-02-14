import re

from typing import Dict, Set, Text, List, Type, Tuple

from pybiz.predicate import Predicate
from pybiz.biz.query.order_by import OrderBy


class GraphQLArguments(object):
    """
    This class is responsible for parsing and normalizing the base arguments
    supplied to a GraphQL query node into the corresponding arguments expected
    by a pybiz Query object.
    """

    _re_order_by = re.compile(r'(\w+)\s+((?:desc)|(?:asc))', re.I)

    @staticmethod
    def extract_arguments_dict(ast_node) -> Dict:
        return {
            arg.name: arg.value for arg in
            getattr(ast_node, 'arguments', ())
        }

    @classmethod
    def parse(cls, biz_class, ast_node) -> 'GraphQLArguments':
        args = cls.extract_arguments_dict(ast_node)
        return cls(
            where=cls._parse_where(biz_class, args.pop('where', None)),
            order_by=cls._parse_order_by(args.pop('order_by', None)),
            offset=cls._parse_offset(args.pop('offset', None)),
            limit=cls._parse_limit(args.pop('limit', None)),
            custom=args  # `custom` is whatever remains in args
        )

    @classmethod
    def _parse_order_by(cls, order_by_strs: Tuple[Text]) -> Tuple[OrderBy]:
        order_by_strs = order_by_strs or []
        order_by_list = []
        for order_by_str in order_by_strs:
            match = self._re_order_by.match(order_by_str)
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
        biz_class: Type['Resource'],
        predicate_strings: List[Text],
    ) -> List['Predicate']:
        if isinstance(predicate_strings, str):
            predicate_strings = [predicate_strings]
        return Predicate.reduce_and(*[
            biz_class.pybiz.predicate_parser.parse(pred_str)
            for pred_str in (predicate_strings or [])
        ])

    def __init__(self, where, order_by, offset, limit, custom: Dict):
        self.where = where
        self.order_by = order_by
        self.offset = offset
        self.limit = limit
        self.custom = custom

    def __getattr__(self, attr):
        return self.custom.get(attr)
