from typing import Text

from pybiz.util.misc_functions import is_sequence, get_class_name
from pybiz.schema import String
from pybiz.predicate import (
    OP_CODE,
    OP_CODE_2_DISPLAY_STRING,  # TODO: Turn OP_CODE into proper enum
    Predicate,
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
        if query.params.get('select'):
            substrings.append('SELECT (')
            substrings.extend(self._build_substring_select_body(query, indent))
            substrings.append(')')

        if query.params.get('where'):
            substrings.extend(self._build_substring_where(query))
        if 'order_by' in query.params:
            substrings.append(self._build_substring_order_by(query))
        if 'offset' in query.params:
            substrings.append(self._build_substring_offset(query))
        if 'limit' in query.params:
            substrings.append(self._build_substring_limit(query))

        return '\n'.join([f'{indent * " "}{s}' for s in substrings])

    def _build_substring_select_head(self, query):
        if query.params.get('select'):
            return f'FROM {get_class_name(query.biz_class)}'
        else:
            return f'FROM {get_class_name(query.biz_class)}'

    def _build_substring_where(self, query):
        substrings = []
        substrings.append('WHERE (')
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
            substrings.append(')')

        return substrings

    def _build_substring_cond_predicate(self, query, predicate, indent=1):
        s_op_code = OP_CODE_2_DISPLAY_STRING[predicate.op]
        s_biz_class = get_class_name(query.biz_class)
        s_field = predicate.field.name
        s_value = str(predicate.value)
        if isinstance(predicate.field, String):
            s_value = s_value.replace('"', '\"')
            s_value = f'"{s_value}"'
        return f'{indent * " "} {s_biz_class}.{s_field} {s_op_code} {s_value}'

    def _build_substring_bool_predicate(self, predicate, indent=1):
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
            f'{indent * " "}   {s_lhs} {s_op_code}',
            f'{indent * " "}   {s_rhs}',
            f'{indent * " "} )',
        ]

    def _build_substring_select_body(self, query, indent: int):
        substrings = []
        resolvers = query.biz_class.pybiz.resolvers
        resolver_queries = query.params.get('select', {}).values()
        if resolver_queries:
            resolver_queries = sorted(
                resolver_queries,
                key=lambda query: (
                    query.resolver.priority(),
                    query.resolver.name,
                    query.resolver.required,
                    query.resolver.private,
                )
            )
            for resolver_query in resolver_queries:
                resolver = resolver_query.resolver
                target = resolver.target
                if target is None:
                    continue

                if resolver.name in target.resolvers.fields:
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

        return substrings

    def _build_substring_selected_field(self, query, indent: int):
        s_name = query.resolver.name
        s_type = get_class_name(query.resolver.field)
        return f'-  {s_name}: {s_type}'

    def _build_substring_selected_resolver(self, query, indent: int):
        substrings = []

        s_name = query.resolver.name
        s_target = None
        if query.resolver.target:
            s_target = get_class_name(query.resolver.target)

        if s_target is None:
            return substrings

        resolver = query.resolver
        if resolver.target:
            s_biz_class = get_class_name(resolver.target)
            s_target = f'List[{s_target}]' if resolver.many else s_target
            substrings.append(f'-  {s_name}: {s_target} ->')
        else:
            substrings.append(f'-  {s_name} ->')

        substrings[0]  += ' ' + self.fprintf(query, indent=indent+5).lstrip()
        #substrings.append(self.fprintf(query, indent=indent+5))
        #substrings.append(f'   )')

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
