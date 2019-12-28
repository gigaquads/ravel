import re
import pickle
import codecs

from functools import reduce
from collections import defaultdict
from typing import Dict, Set, Text, List, Type, Tuple
from threading import local

import sqlparse

from sqlparse.sql import (
    Statement, Parenthesis, Token, Comparison,
    Identifier,
)

from appyratus.enum import Enum
from appyratus.utils import DictObject
from appyratus.schema import RangeConstraint, ConstantValueConstraint

from pybiz.util.misc_functions import flatten_sequence
from pybiz.schema import Enum as EnumField
from pybiz.constants import ID_FIELD_NAME, REV_FIELD_NAME
from pybiz.biz.resolver.resolver_property import ResolverProperty

from .biz.util import is_biz_object


OP_CODE = Enum(
    EQ='eq',
    NEQ='neq',
    GT='gt',
    LT='lt',
    GEQ='geq',
    LEQ='leq',
    INCLUDING='in',
    EXCLUDING='ex',
    AND='and',
    OR='or',
)

OP_CODE_2_DISPLAY_STRING = {
    OP_CODE.EQ: '==',
    OP_CODE.NEQ: '!=',
    OP_CODE.GT: '>',
    OP_CODE.LT: '<',
    OP_CODE.GEQ: '>=',
    OP_CODE.LEQ: '<=',
    OP_CODE.INCLUDING: 'in',
    OP_CODE.EXCLUDING: 'not in',
    OP_CODE.AND: '&&',
    OP_CODE.OR: '||',
}

DISPLAY_STRING_2_OP_CODE = {
    v: k for k, v in OP_CODE_2_DISPLAY_STRING.items()
}

TYPE_BOOLEAN = 1
TYPE_CONDITIONAL = 2

# globals used by PredicateParser:
CONDITIONAL_OPERATORS = frozenset({'==', '!=', '>', '>=', '<', '<='})
BOOLEAN_OPERATORS = frozenset({'&&', '||'})

RE_INT = re.compile(r'\d+')
RE_FLOAT = re.compile(r'\d*(\.\d+)')
RE_STRING = re.compile(r'\'.+\'')


class Predicate(object):
    TYPE_BOOLEAN = TYPE_BOOLEAN
    TYPE_CONDITIONAL = TYPE_CONDITIONAL
    AND_FUNC = lambda x, y: x & y
    OR_FUNC = lambda x, y: x | y

    def __init__(self, code):
        self.code = code
        self.fields = set()
        self.targets = set()
        self.unbound_predicates = set()

    def serialize(self) -> Text:
        """
        Return the Predicate as a base64 encoded pickle. This is used, for
        instance, by pybiz gRPC instrumentation, for passing these objects over
        the line.
        """
        return codecs.encode(pickle.dumps(self), "base64").decode()

    @staticmethod
    def deserialize(obj) -> 'Predicate':
        """
        Return the Predicate from a base64 encoded pickle. This is used, for
        instance, by pybiz gRPC instrumentation, for passing these objects over
        the line.
        """
        if isinstance(obj, str):
            return pickle.loads(codecs.decode(obj.encode(), 'base64'))
        elif isinstance(obj, Predicate):
            return obj
        else:
            raise ValueError(str(obj))

    def dump(self):
        raise NotImplementedError()

    def bind(self, data: Dict[Text, 'BizObject']) -> 'Predicate':
        raise NotImplementedError()

    @classmethod
    def load(cls, biz_class: Type['BizObject'], data: Dict):
        if data['code'] == TYPE_CONDITIONAL:
            return ConditionalPredicate.load(biz_class, data)
        elif data['code'] == TYPE_BOOLEAN:
            return BooleanPredicate.load(biz_class, data)

    @classmethod
    def reduce_and(cls, *predicates) -> 'Predicate':
        return cls._reduce(cls.AND_FUNC, predicates)

    @classmethod
    def reduce_or(cls, *predicates) -> 'Predicate':
        return cls._reduce(cls.OR_FUNC, predicates)

    @staticmethod
    def _reduce(func, predicates: List['Predicate']) -> 'Predicate':
        predicates = flatten_sequence(p for p in predicates if p is not None)
        if not predicates:
            return None
        if len(predicates) == 1:
            return list(predicates)[0]
        else:
            return reduce(func, predicates)

    @property
    def is_unbound(self):
        return bool(self.unbound_predicates)

    @property
    def is_conditional_predicate(self):
        return self.code == TYPE_CONDITIONAL

    @property
    def is_boolean_predicate(self):
        return self.code == TYPE_BOOLEAN

    # XXX: deprecated
    """
    def compute_constraints(self) -> 'Constraint':
        constraints = defaultdict(dict)
        self._compute_constraint(self, constraints)
        return constraints

    def _compute_constraint(self, predicate, constraints):
        if predicate.code == TYPE_BOOLEAN:
            self._compute_constraint(predicate.lhs, constraints)
            self._compute_constraint(predicate.rhs, constraints)
        elif predicate.code == TYPE_CONDITIONAL:
            field = predicate.prop.field
            if predicate.op == OP_CODE.EQ:
                constraints[field.name] = ConstantValueConstraint(
                    value=predicate.value,
                    is_negative=False
                )
            elif predicate.op == OP_CODE.NEQ:
                constraints[field.name] = ConstantValueConstraint(
                    value=predicate.value,
                    is_negative=True
                )
            elif predicate.op == OP_CODE.INCLUDING:
                constraints[field.name] = ConstantValueConstraint(
                    value=random.choice(list(predicate.value)),
                    is_negative=False
                )
            elif predicate.op == OP_CODE.EXCLUDING:
                disallowed_values = set(predicate.value)
                if isinstance(field, EnumField):
                    possible_values = set(field.values) - disallowed_values
                    is_enum = True
                else:
                    possible_values = None
                    is_enum = False
                if not is_enum:
                    while True:
                        value = field.generate()
                        if value not in disallowed_values:
                            break
                elif disallowed_values == possible_values:
                    value = None
                else:
                    possible_values = list(possible_values)
                    value = random.choice(possible_values)
                constraints[field.name] = ConstantValueConstraint(
                    value=value,
                    is_negative=True
                )

            con = constraints.setdefault(field.name, RangeConstraint())
            if not con.is_equality_constraint:
                if predicate.op == OP_CODE.LEQ:
                    con.upper_value = predicate.value
                    con.is_upper_inclusive = True
                elif predicate.op == OP_CODE.LT:
                    con.upper_value = predicate.value
                    con.is_upper_inclusive = False
                elif predicate.op == OP_CODE.GEQ:
                    con.lower_value = predicate.value
                    con.is_lower_inclusive = True
                elif predicate.op == OP_CODE.GT:
                    con.lower_value = predicate.value
                    con.is_lower_inclusive = False
        else:
            raise ValueError(
                f'unrecogized predicate type: {predicate.code}'
            )
        return constraints
    """

class ConditionalPredicate(Predicate):
    """
    A `ConditionalPredicate` specifies a comparison between the name of a field
    and a value. The field name is made available through the FieldProperty
    objects that instantiated the predicate.
    """
    def __init__(self, op: Text, prop: 'FieldProperty', value):
        super().__init__(code=TYPE_CONDITIONAL)
        self.op = op
        self.prop = prop
        self.value = value
        self.fields.add(self.prop.field)
        self.targets.add(self.prop.biz_class)

        is_bound = not isinstance(value, ResolverAlias)
        if not is_bound:
            self.unbound_predicates.add(self)

    def __repr__(self):
        return '<{}({})>'.format(
            self.__class__.__name__,
            str(self)[1:-1],
        )

    def __str__(self):
        if self.prop:
            biz_class_name = self.prop.biz_class.__name__
            lhs = f'{biz_class_name}.{self.prop.field.name}'
        else:
            lhs = '[NULL]'

        return f'({lhs} {OP_CODE_2_DISPLAY_STRING[self.op]} {self.value})'

    def __or__(self, other):
        return BooleanPredicate(OP_CODE.OR, self, other)

    def __and__(self, other):
        return BooleanPredicate(OP_CODE.AND, self, other)

    @property
    def field(self):
        return self.prop.field

    def dump(self):
        return {
            'op': self.op,
            'field': self.prop.field.name,
            'value': self.value,
            'code': self.code,
        }

    def bind(self, data: Dict[Text, 'BizObject']) -> 'Predicate':
        resolver_alias = self.value
        source = data[resolver_alias.alias_name]
        self.value = getattr(source, resolver_alias.resolver_name)
        self.unbound_predicates.remove(self)
        return self

    @classmethod
    def load(cls, biz_class: Type['BizObject'], data: Dict):
        field_prop = getattr(biz_class, data['field'])
        return cls(data['op'], field_prop, data['value'])


class BooleanPredicate(Predicate):
    """
    A `BooleanPredicate` is used for combining children `Predicate` objects in
    a boolean expression, like (User._name == 'foo') & (User.smell == 'stink').
    LSH, and LHS stand for "left-hand side" and "right-hand side", respectively.
    """
    def __init__(self, op, lhs: 'Predicate', rhs: 'Predicate' = None):
        super().__init__(code=TYPE_BOOLEAN)
        self.op = op
        self.lhs = lhs
        self.rhs = rhs
        self._unbound_predicates.update(lhs.unbound_predicates)
        self._unbound_predicates.update(rhs.unbound_predicates)
        if lhs.code == TYPE_CONDITIONAL:
            self.fields.add(lhs.prop.field)
        if rhs.code == TYPE_CONDITIONAL:
            self.fields.add(rhs.prop.field)

    def __or__(self, other):
        return BooleanPredicate(OP_CODE.OR, self, other)

    def __and__(self, other):
        return BooleanPredicate(OP_CODE.AND, self, other)

    def __str__(self):
        return self._build_string(self)

    def __repr__(self):
        return '<{}({} {} {})>'.format(
            self.__class__.__name__, self.lhs, self.op, self.rhs,
        )

    def _build_string(self, p, depth=0):
        substr = ''

        if isinstance(p.lhs, BooleanPredicate):
            substr += self._build_string(p.lhs, depth+1)
        else:
            substr += str(p.lhs)

        substr += f' {OP_CODE_2_DISPLAY_STRING[p.op]} '

        if isinstance(p.rhs, BooleanPredicate):
            substr += self._build_string(p.rhs, depth+1)
        else:
            substr += str(p.rhs)

        return f'({substr})'

    def dump(self):
        return {
            'op': self.op,
            'lhs': self.lhs.dump(),
            'rhs': self.rhs.dump(),
            'code': self.code,
        }

    def bind(self, data: Dict[Text, 'BizObject']) -> 'Predicate':
        self.lhs.bind(data)
        self.rhs.bind(data)
        return self

    @classmethod
    def load(cls, biz_class: Type['BizObject'], data: Dict):
        return cls(
            data['op'],
            Predicate.load(biz_class, data['lhs']),
            Predicate.load(biz_class, data['rhs']),
        )


class PredicateParser(object):

    pybiz_field_name_transform_inversions = {
        'id': ID_FIELD_NAME,
        'rev': REV_FIELD_NAME,
    }

    class Operand(object):
        def __init__(self, op_code, arity):
            self.op_code = op_code
            self.arity = arity

    def __init__(self, biz_class):
        self._biz_class = biz_class

    def parse(self, source: Text):
        source = f'({source})'
        stmt = sqlparse.parse(source)[0]
        predicate = self._parse_predicate(stmt[0])
        return predicate

    def _parse_predicate(self, paren: Parenthesis):
        predicate_stack = []
        op_stack = []
        for token in paren:
            if isinstance(token, Parenthesis):
                predicate = self._parse_predicate(token)
                predicate_stack.append(predicate)
                if op_stack:
                    op = op_stack[-1]
                    if len(predicate_stack) >= op.arity:
                        op_stack.pop()
                        args = [predicate_stack.pop() for i in range(op.arity)]
                        if op.op_code == 'and':
                            predicate_stack.append(Predicate.reduce_and(*args))
                        elif op.op_code == 'or':
                            predicate_stack.append(Predicate.reduce_or(*args))
                        else:
                            raise Exception()
            elif token.is_keyword:
                value = token.value.lower()
                if value in {'and', 'or'}:
                    op_stack.append(self.Operand(value, 2))
                elif value == 'in':
                    predicate_stack.append(self._parse_in_predicate(paren))
            elif isinstance(token, Comparison):
                return self._parse_comparison(token)

        return predicate_stack[0] if predicate_stack else None

    def _parse_comparison(self, comp: Comparison):
        if isinstance(comp.left, Identifier):
            ident = comp.left.value
            target = comp.right.value
        else:
            ident = comp.right.value
            target = comp.left.value

        target = target.strip("'")
        ident = self.pybiz_field_name_transform_inversions.get(ident, ident)
        prop = getattr(self._biz_class, ident)

        op_code = None
        for token in comp:
            if token.ttype == sqlparse.tokens.Comparison:
                op_code = token.value
                break

        if op_code == '=':
            return prop == target
        if op_code == '!=':
            return prop != target
        if op_code == '<':
            return prop < target
        if op_code == '>':
            return prop > target
        if op_code == '>=':
            return prop >= target
        if op_code == '<=':
            return prop <= target

        raise Exception()

    def _parse_in_predicate(self, paren: Parenthesis):
        ident = None
        value_list = []
        is_negative = False
        for token in paren:
            if token.is_keyword and token.value.lower() == 'not':
                is_negative = not is_negative
            if isinstance(token, Identifier):
                if ident is None:
                    ident = token.value
                else:
                    ident = self.pybiz_field_name_transform_inversions.get(
                        ident, ident
                    )
                    prop = getattr(self._biz_class, ident)
                    value_list = [
                        prop.field.process(x.strip("'").strip())[0]
                        for x in token.value[1:-1].split(',')
                    ]
                    if is_negative:
                        return prop.excluding(value_list)
                    else:
                        return prop.including(value_list)


class AliasFactory(object):
    def __getattr__(self, name):
        return Alias(name)


class Alias(object):
    def __init__(self, name: Text):
        self._name = name

    def __getattr__(self, resolver_name):
        return ResolverAlias(self._name, resolver_name)


class ResolverAlias(object):
    def __init__(self, alias_name: Text, resolver_name: Text):
        self._alias_name = alias_name
        self._resolver_name = resolver_name

    @property
    def alias_name(self):
        return self._alias_name

    @property
    def resolver_name(self):
        return self._resolver_name
