from typing import Text

from pybiz.util.misc_functions import is_sequence, get_class_name
from pybiz.schema import String, Id
from pybiz.constants import ID_FIELD_NAME, REV_FIELD_NAME
from pybiz.predicate import (
    OP_CODE,
    OP_CODE_2_DISPLAY_STRING,  # TODO: Turn OP_CODE into proper enum
    Predicate,
    ResolverAlias
)


class QueryPrinter(object):
    """
    Pretty prints a Query object, recursively.
    """

    def printf(self, query: 'Query'):
        """
        Pretty print a query object to stdout.
        """
        print(self.fprintf(query))

    def fprintf(self, query: 'Query', indent=0) -> Text:
        """
        Generate the prettified display string and return it.
        """
        substrings = []

        substrings.append(self._build_substring_select_head(query))
        if query.params.get('select') or query.params.get('subqueries'):
            substrings.append('SELECT')
            substrings.extend(self._build_substring_select_body(query, indent))
            if query.params.alias:
                substrings.append(f'AS {query.params.alias}')
        else:
            substrings[-1] += ' SELECT VOID'

        if query.params.get('where'):
            substrings.extend(self._build_substring_where(query))
        if query.params.get('order_by'):
            substrings.append(self._build_substring_order_by(query))
        if query.params.get('offset') is not None:
            substrings.append(self._build_substring_offset(query))
        if query.params.get('limit') is not None:
            substrings.append(self._build_substring_limit(query))

        substrings.append(';')

        return '\n'.join([f'{indent * " "}{s.rstrip()}' for s in substrings])

    def _build_substring_select_head(self, query):
        if query.params.get('select'):
            return f'FROM {get_class_name(query.biz_class)}'
        else:
            return f'FROM {get_class_name(query.biz_class)}'

    def _build_substring_where(self, query):
        substrings = []
        substrings.append('WHERE')
        predicates = query.params['where']
        if isinstance(predicates, Predicate):
            predicates = [predicates]
        for idx, predicate in enumerate(predicates):
            if predicate.is_boolean_predicate:
                substrings.extend(
                    self._build_substring_bool_predicate(query, predicate)
                )
            else:
                assert predicate.is_conditional_predicate
                substrings.append(
                    self._build_substring_cond_predicate(query, predicate)
                )

        return substrings

    def _build_substring_cond_predicate(self, query, predicate, indent=1):
        s_op_code = OP_CODE_2_DISPLAY_STRING[predicate.op]
        s_biz_class = get_class_name(query.biz_class)
        s_field = predicate.field.name
        if isinstance(predicate.value, ResolverAlias):
            alias = predicate.value
            s_value = f'{alias.alias_name}.{alias.resolver_name}'
        else:
            s_value = f'{predicate.value}'
            if isinstance(predicate.field, String):
                s_value = s_value.replace('"', '\"')
                s_value = f'"{s_value}"'
        return f'{indent * " "} {s_biz_class}.{s_field} {s_op_code} {s_value}'

    def _build_substring_bool_predicate(self, query, predicate, indent=1):
        s_op_code = OP_CODE_2_DISPLAY_STRING[predicate.op]
        if predicate.lhs.is_boolean_predicate:
            s_lhs = self._build_substring_bool_predicate(
                query, predicate.lhs, indent=indent+1
            )
        else:
            s_lhs = self._build_substring_cond_predicate(
                query, predicate.lhs, indent=indent+1
            )

        if predicate.rhs.is_boolean_predicate:
            s_rhs = self._build_substring_bool_predicate(
                query, predicate.rhs, indent=indent+1
            )
        else:
            s_rhs = self._build_substring_cond_predicate(
                query, predicate.rhs, indent=indent+1
            )
        return [
            f'{indent * " "} (',
            f'{indent * " "}{s_lhs} {s_op_code}',
            f'{indent * " "}{s_rhs}',
            f'{indent * " "} ) ',
        ]

    def _build_substring_select_body(self, query, indent: int):
        substrings = []
        resolvers = query.biz_class.pybiz.resolvers
        resolver_queries = query.params.get('select', {})
        resolver_queries = sorted(
            resolver_queries.values(),
            key=lambda query: (
                query.resolver.priority(),
                query.resolver.name,
                query.resolver.required,
                query.resolver.private,
            )
        )
        for resolver_query in resolver_queries:
            resolver = resolver_query.resolver
            target = resolver.target_biz_class
            if target is None:
                continue

            if resolver.name in target.pybiz.resolvers.fields:
                if resolver.name in (ID_FIELD_NAME, REV_FIELD_NAME):
                    continue
                substrings.append(
                    self._build_substring_selected_field(
                        resolver_query, indent
                    )
                )
            else:
                substrings.extend(
                    self._build_substring_selected_resolver(
                        resolver_query, indent
                    )
                )

        if query.params.subqueries:
            for name, subquery in query.params.subqueries.items():
                if not subquery.options['first']:
                    target = f'List[{get_class_name(subquery.biz_class)}]'
                else:
                    target = f'{get_class_name(subquery.biz_class)}'
                substrings.append(f'- {name}: {target} ->')
                substrings.append(self.fprintf(subquery, indent+5).rstrip(';'))

        return substrings

    def _build_substring_selected_field(self, query, indent: int):
        field = query.resolver.field
        s_name = query.resolver.name
        if isinstance(field, Id):
            s_type = get_class_name(field.target_field_class)
        else:
            s_type = get_class_name(field)
        return f'- {s_name}: {s_type}'

    def _build_substring_selected_resolver(self, query, indent: int):
        substrings = []

        s_name = query.resolver.name
        s_target = None
        if query.resolver.target_biz_class:
            s_target = get_class_name(query.resolver.target_biz_class)

        if s_target is None:
            return substrings

        resolver = query.resolver
        if resolver.target_biz_class:
            s_biz_class = get_class_name(resolver.target_biz_class)
            s_target = f'List[{s_target}]' if resolver.many else s_target
            substrings.append(f'- {s_name}: {s_target} ->')
        else:
            substrings.append(f'- {s_name} ->')

        substrings[0]  += ' ' + self.fprintf(
            query, indent=indent+5
        ).lstrip().rstrip(';')

        return substrings

    def _build_substring_order_by(self, query):
        order_by = query.params.get('order_by', [])
        if not is_sequence(order_by):
            order_by = [order_by]
        return (
            'ORDER BY (' + ', '.join(
                f'{x.key} {"DESC" if x.desc else "ASC"}'
                for x in order_by
            ) + ')'
        )

    def _build_substring_offset(self, query):
        return f'OFFSET {query.params["offset"]}'

    def _build_substring_limit(self, query):
        return f'LIMIT {query.params["limit"]}'
