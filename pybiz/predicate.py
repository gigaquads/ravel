import re
import pickle
import codecs

from functools import reduce
from collections import defaultdict
from typing import Dict, Set, Text, List, Type, Tuple
from threading import local

from pyparsing import Literal, Regex, Forward, Optional, Word
from appyratus.enum import Enum
from appyratus.utils import DictObject
from appyratus.schema import RangeConstraint, ConstantValueConstraint


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
    OP_CODE.GT: '>=',
    OP_CODE.LT: '<=',
    OP_CODE.GEQ: '>=',
    OP_CODE.LEQ: '<=',
    OP_CODE.INCLUDING: 'in',
    OP_CODE.EXCLUDING: 'not in',
    OP_CODE.AND: '&&',
    OP_CODE.OR: '||',
}

TYPE_BOOLEAN = 1
TYPE_CONDITIONAL = 2

# globals used by PredicateParser:
CONDITIONAL_OPERATORS = frozenset({'==', '!=', '>', '>=', '<', '<='})
BOOLEAN_OPERATORS = frozenset({'&&', '||'})

RE_INT = re.compile(r'\d+')
RE_FLOAT = re.compile(r'\d*(\.\d+)')
RE_STRING = re.compile(r'(\'|").+(\'|")')


class Predicate(object):
    TYPE_BOOLEAN = TYPE_BOOLEAN
    TYPE_CONDITIONAL = TYPE_CONDITIONAL
    AND_FUNC = lambda x, y: x & y
    OR_FUNC = lambda x, y: x | y

    def __init__(self, code):
        self.code = code
        self.fields = set()

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

    @classmethod
    def load(cls, biz_class: Type['BizObject'], data: Dict):
        if data['code'] == TYPE_CONDITIONAL:
            return ConditionalPredicate.load(biz_class, data)
        elif data['code'] == TYPE_BOOLEAN:
            return BooleanPredicate.load(biz_class, data)

    @classmethod
    def reduce_and(cls, predicates: List['Predicate']) -> 'Predicate':
        return cls._reduce(cls.AND_FUNC, predicates)

    @classmethod
    def reduce_or(cls, predicates: List['Predicate']) -> 'Predicate':
        return cls._reduce(cls.OR_FUNC, predicates)

    @staticmethod
    def _reduce(func, predicates: List['Predicate']) -> 'Predicate':
        predicates = [p for p in predicates if p is not None]
        if not predicates:
            return None
        if len(predicates) == 1:
            return predicates[0]
        else:
            return reduce(func, predicates)

    @property
    def is_conditional_predicate(self):
        return self.code == TYPE_CONDITIONAL

    @property
    def is_boolean_predicate(self):
        return self.code == TYPE_BOOLEAN

    def compute_constraints(self) -> 'Constraint':
        constraints = defaultdict(dict)
        self._compute_constraint(self, constraints)
        return constraints

    def _compute_constraint(self, predicate, constraints):
        if predicate.code == TYPE_BOOLEAN:
            self._compute_constraint(predicate.lhs, constraints)
            self._compute_constraint(predicate.rhs, constraints)
        elif predicate.code == TYPE_CONDITIONAL:
            field = predicate.fprop.field
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
            # TODO: INCLUDING and EXCLUDING predicates

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


class ConditionalPredicate(Predicate):
    """
    A `ConditionalPredicate` specifies a comparison between the name of a field
    and a value. The field name is made available through the FieldProperty
    objects that instantiated the predicate.
    """
    def __init__(self, op: Text, prop: 'FieldProperty', value):
        super().__init__(code=TYPE_CONDITIONAL)
        self.op = op
        self.fprop = prop
        self.value = value
        self.fields.add(self.fprop.field)

    def __repr__(self):
        return '<{}({})>'.format(
            self.__class__.__name__,
            str(self)[1:-1],
        )

    def __str__(self):
        if self.fprop:
            biz_class_name = self.fprop.biz_class.__name__
            lhs = f'{biz_class_name}.{self.fprop.field.name}'
        else:
            lhs = '[NULL]'

        return f'({lhs} {OP_CODE_2_DISPLAY_STRING[self.op]} {self.value})'

    def __or__(self, other):
        return BooleanPredicate(OP_CODE.OR, self, other)

    def __and__(self, other):
        return BooleanPredicate(OP_CODE.AND, self, other)

    @property
    def field(self):
        return self.fprop.field

    def dump(self):
        return {
            'op': self.op,
            'field': self.fprop.field.name,
            'value': self.value,
            'code': self.code,
        }

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
        if lhs.code == TYPE_CONDITIONAL:
            self.fields.add(lhs.fprop.field)
        if rhs.code == TYPE_CONDITIONAL:
            self.fields.add(rhs.fprop.field)

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

    @classmethod
    def load(cls, biz_class: Type['BizObject'], data: Dict):
        return cls(
            data['op'],
            Predicate.load(biz_class, data['lhs']),
            Predicate.load(biz_class, data['rhs']),
        )


class PredicateParser(object):
    """
    """

    def __init__(self):
        self._stack = []
        self._biz_class = None
        self._init_grammar()

    def _init_grammar(self):
        self._grammar = DictObject()
        self._grammar.ident = Regex(r'[a-zA-Z_]\w*')
        self._grammar.number = Regex(r'\d*(\.\d+)?')
        self._grammar.string = Regex(r"'.+'")
        self._grammar.conditional_value = (
           # self._grammar.number |
            self._grammar.string
        )
        self._grammar.conditional_operator = reduce(
            lambda x, y: x | y, (Literal(op) for op in CONDITIONAL_OPERATORS)
        )
        self._grammar.boolean_operator = reduce(
            lambda x, y: x | y, (Literal(op) for op in BOOLEAN_OPERATORS)
        )
        self._grammar.lparen = Literal('(')
        self._grammar.rparen = Literal(')')
        self._grammar.conditional_predicate = (
            self._grammar.ident.setResultsName('field') +
            self._grammar.conditional_operator.setResultsName('op') +
            self._grammar.conditional_value.setResultsName('value')
        ).addParseAction(self._on_parse_conditional_predicate)

        self._grammar.boolean_predicate = Forward().addParseAction(
            self._on_parse_boolean_predicate
        )
        self._grammar.any_predicate = (
            self._grammar.lparen
            + (self._grammar.boolean_predicate
                | self._grammar.conditional_predicate)
            + self._grammar.rparen
        )
        self._grammar.boolean_predicate << (
            Optional(self._grammar.lparen)
            + (
                self._grammar.any_predicate
                + self._grammar.boolean_operator.setResultsName('operator')
                + self._grammar.any_predicate
            )
            + Optional(self._grammar.rparen)
        )
        self._grammar.root = (
            (
                Optional(self._grammar.lparen)
                + self._grammar.conditional_predicate
                + Optional(self._grammar.rparen)
            )
            | self._grammar.boolean_predicate
        )

    def _on_parse_conditional_predicate(self, source: Text, loc: int, tokens: Tuple):
        # TODO: further process "value" as list or other dtype
        fprop = getattr(self._biz_class, tokens['field'])
        value = tokens['value']
        if RE_STRING.match(value):
            value = value[1:-1]
        elif RE_INT.match(value):
            value = int(value)
        elif RE_FLOAT.match(value):
            value = float(value)
        else:
            raise ValueError()
        predicate = ConditionalPredicate(op, fprop, value)
        self._stack.append(predicate)

    def _on_parse_boolean_predicate(self, source: Text, loc: int, tokens: Tuple):
        lhs = self._stack.pop()
        rhs = self._stack.pop()
        if tokens['op'] == '&&':
            self._stack.append(lhs & rhs)
        elif tokens['op'] == '||':
            self._stack.append(lhs | rhs)

    def parse(self, biz_class, source: Text) -> 'Predicate':
        self._biz_class = biz_class
        self._stack.clear()
        self._grammar.root.parseString(source)
        predicate = self._stack[-1]
        return predicate
