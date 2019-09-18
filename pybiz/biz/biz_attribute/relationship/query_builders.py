from typing import Text, Type, Tuple, Dict, Set, Callable, List


from pybiz.predicate import Predicate
from pybiz.util.misc_functions import is_bizobj, is_bizlist
from pybiz.util.loggers import console

from ...query import Query


class QueryBuilder(object):
    def __init__(
        self,
        source: 'BizThing',
        target_biz_class: Type['BizObject'],
        predicate: Predicate,
    ):
        self.source = source
        self.target_biz_class = target_biz_class
        self.predicate = predicate

    def build_query(
        self,
        select=None,
        where=None,
        limit=None,
        offset=None,
        order_by=None,
        custom: Dict = None,
    ) -> Query:
        """
        For each "join" function given to a Relationship, a QueryBuilder is
        responsible for building a Query object.
        """
        where_predicate = self.build_where_predicate(where)
        selectors = self.build_selectors(select, where_predicate)
        return self.target_biz_class.query(
            select=selectors, where=where_predicate, offset=offset,
            limit=limit, order_by=order_by, custom=custom, execute=False
        )

    def build_where_predicate(self, additional_predicates: Tuple) -> Predicate:
        if additional_predicates:
            return Predicate.reduce_and(self.predicate, *additional_predicates)
        else:
            return self.predicate

    def build_selectors(self, select: Set, where: Predicate) -> Set:
        # merge custom selectors into base selectors
        if select is None:
            select = set()
        elif not isinstance(select, set):
            select = set(select)
        return select

        selectors = target_biz_class.pybiz.default_selectors.copy()
        selectors.update(f.name for f in where.fields)
        selectors.update(select)
        return selectors


class DynamicQueryBuilder(QueryBuilder):
    """
    DynamicQueryBuilders correspond to "join" functions with a return signature
    of (Type[BizObject], Predicate?).
    """

    def __init__(
        self,
        source: 'BizThing',
        target_biz_class: Type['BizObject'],
        predicate: Predicate = None
    ):
        super().__init__(source, target_biz_class, predicate)


class StaticQueryBuilder(QueryBuilder):
    """
    StaticQueryBuilders correspond to "join" functions with return signatures
    in the format (FieldProperty, FieldProperty, Predicate?)
    """

    def __init__(
        self,
        source: 'BizThing',
        source_fprop: 'FieldProperty',
        target_fprop: 'FieldProperty',
        predicate: Predicate = None
    ):
        super().__init__(source, target_fprop.biz_class, predicate)
        self.target_fprop = target_fprop
        self.source_fprop = source_fprop
        self.source_fname = source_fprop.field.name
        self.target_fname = target_fprop.field.name
        self.source_biz_class = self.source_fprop.biz_class

    def build_where_predicate(self, additional_predicates: List) -> Predicate:
        target_field_value = getattr(self.source, self.source_fprop.field.name)
        where_predicate = None

        if is_bizobj(self.source):
            where_predicate = (self.target_fprop == target_field_value)
        elif is_bizlist(self.source):
            where_predicate = (self.target_fprop.including(target_field_value))
        else:
            raise TypeError('TODO: raise custom exception')

        if self.predicate:
            where_predicate &= self.predicate

        if additional_predicates:
            return Predicate.reduce_and(where_predicate, *additional_predicates)
        else:
            return where_predicate
