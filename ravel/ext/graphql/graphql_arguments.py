import re

from typing import Dict, Set, Text, List, Type, Tuple, Union

from ravel.query.predicate import Predicate
from ravel.query.order_by import OrderBy


class GraphqlArguments(object):
    """
    This class is responsible for parsing and normalizing the base arguments
    supplied to a GraphQL query node into the corresponding arguments expected
    by a ravel Query object.
    """

    @staticmethod
    def extract_arguments_dict(ast_node) -> Dict:
        return {
            arg.name: arg.value for arg in
            getattr(ast_node, 'arguments', ())
        }

    @classmethod
    def parse(cls, resource_type, ast_node) -> 'GraphqlArguments':
        args = cls.extract_arguments_dict(ast_node)
        return cls(
            where=cls._parse_where(resource_type, args.pop('where', None)),
            order_by=cls._parse_order_by(args.pop('order_by', None)),
            offset=cls._parse_offset(args.pop('offset', None)),
            limit=cls._parse_limit(args.pop('limit', None)),
            custom=args  # `custom` is whatever remains in args
        )

    @classmethod
    def _parse_order_by(cls, order_by_strs: Union[Text, Tuple[Text]]) -> Tuple[OrderBy]:
        if isinstance(order_by_strs, str):
            order_by_strs = [order_by_strs]
        else:
            order_by_strs = order_by_strs or []

        order_bys = []

        for order_by_str in order_by_strs:
            parts = order_by_str.strip().split()

            if len(parts) == 1:
                key = parts
                desc = False
            elif len(parts) == 2:
                key = parts[0]

                ordering = parts[1].lower()
                if ordering == 'desc':
                    desc = True
                elif ordering == 'asc':
                    desc = False
                else:
                    raise ValueError(
                        f'unrecognized "order by" value: {order_by_str}'
                    )

                desc = True if parts[1].lower() == 'desc' else False
                order_by = OrderBy.load({'key': key, 'desc': desc})
                order_bys.append(order_by)

        return tuple(order_bys)

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
        resource_type: Type['Resource'],
        predicate_strings: List[Text],
    ) -> List['Predicate']:
        if isinstance(predicate_strings, str):
            predicate_strings = [predicate_strings]
        return Predicate.reduce_and(*[
            resource_type.ravel.predicate_parser.parse(pred_str)
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
