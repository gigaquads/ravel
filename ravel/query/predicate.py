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

from ravel.util.misc_functions import (
    flatten_sequence, is_sequence, get_class_name
)
from ravel.schema import Enum as EnumField
from ravel.constants import ID, REV, OP_CODE
from ravel.util import is_resource, is_batch


PREDICATE_TYPE = Enum(
    BOOLEAN=1,
    CONDITIONAL=2,
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

NON_SCALAR_OP_CODES = {
    OP_CODE.INCLUDING,
    OP_CODE.EXCLUDING,
}

# regular expressions
RE_INT = re.compile(r'\d+')
RE_FLOAT = re.compile(r'\d*(\.\d+)')
RE_STRING = re.compile(r'\'.+\'')


class Predicate(object):
    TYPE = PREDICATE_TYPE
    AND_FUNC = lambda x, y: x & y
    OR_FUNC = lambda x, y: x | y

    def __init__(self, code):
        self.code = code
        self.fields = set()
        self.targets = set()

    def serialize(self) -> Text:
        """
        Return the Predicate as a base64 encoded pickle. This is used, for
        instance, by ravel gRPC instrumentation, for passing these objects over
        the line.
        """
        return codecs.encode(pickle.dumps(self), "base64").decode()

    @staticmethod
    def deserialize(obj) -> 'Predicate':
        """
        Return the Predicate from a base64 encoded pickle. This is used, for
        instance, by ravel gRPC instrumentation, for passing these objects over
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
    def load(cls, resource_type: Type['Resource'], data: Dict):
        if data['code'] == PREDICATE_TYPE.CONDITIONAL:
            return ConditionalPredicate.load(resource_type, data)
        elif data['code'] == PREDICATE_TYPE.BOOLEAN:
            return BooleanPredicate.load(resource_type, data)

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
    def is_conditional_predicate(self):
        return self.code == PREDICATE_TYPE.CONDITIONAL

    @property
    def is_boolean_predicate(self):
        return self.code == PREDICATE_TYPE.BOOLEAN


class ConditionalPredicate(Predicate):
    """
    A `ConditionalPredicate` specifies a comparison between the name of a field
    and a value. The field name is made available through the FieldProperty
    objects that instantiated the predicate.
    """
    def __init__(self, op: Text, prop: 'FieldProperty', value):
        super().__init__(code=PREDICATE_TYPE.CONDITIONAL)
        self.op = op
        self.prop = prop
        self.value = value
        self.fields.add(self.prop.resolver.field)
        self.targets.add(self.prop.resolver.owner)
        self.is_scalar = op not in NON_SCALAR_OP_CODES

    def __repr__(self):
        return f'{get_class_name(self)}({str(self)[1:-1]})'

    def __str__(self):
        if self.prop:
            res_class_name = get_class_name(self.prop.resolver.owner)
            lhs = f'{res_class_name}.{self.prop.resolver.field.name}'
        else:
            lhs = '[NULL]'

        val = self.value

        return f'({lhs} {OP_CODE_2_DISPLAY_STRING[self.op]} {val})'

    def __or__(self, other):
        return BooleanPredicate(OP_CODE.OR, self, other)

    def __and__(self, other):
        return BooleanPredicate(OP_CODE.AND, self, other)

    @property
    def field(self):
        return self.prop.resolver.field

    def dump(self):
        return {
            'op': self.op,
            'field': self.prop.resolver.field.name,
            'value': self.value,
            'code': self.code,
        }

    @classmethod
    def load(cls, resource_type: Type['Resource'], data: Dict):
        field_prop = getattr(resource_type, data['field'])
        return cls(data['op'], field_prop, data['value'])


class BooleanPredicate(Predicate):
    """
    A `BooleanPredicate` is used for combining children `Predicate` objects in
    a boolean expression, like (User._name == 'foo') & (User.smell == 'stink').
    LSH, and LHS stand for "left-hand side" and "right-hand side", respectively.
    """
    def __init__(self, op, lhs: 'Predicate', rhs: 'Predicate' = None):
        super().__init__(code=PREDICATE_TYPE.BOOLEAN)
        self.op = op
        self.lhs = lhs
        self.rhs = rhs
        if lhs.code == PREDICATE_TYPE.CONDITIONAL:
            self.fields.add(lhs.prop.resolver.field)
        if rhs.code == PREDICATE_TYPE.CONDITIONAL:
            self.fields.add(rhs.prop.resolver.field)

    def __or__(self, other):
        return BooleanPredicate(OP_CODE.OR, self, other)

    def __and__(self, other):
        return BooleanPredicate(OP_CODE.AND, self, other)

    def __str__(self):
        return self._build_string(self)

    def __repr__(self):
        return f'{get_class_name(self)}({self.lhs} {self.op} {self.rhs})'

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
    def load(cls, resource_type: Type['Resource'], data: Dict):
        return cls(
            data['op'],
            Predicate.load(resource_type, data['lhs']),
            Predicate.load(resource_type, data['rhs']),
        )


class PredicateParser(object):

    ravel_field_name_transform_inversions = {
        'id': ID,
        'rev': REV,
    }

    class Operand(object):
        def __init__(self, op_code, arity):
            self.op_code = op_code
            self.arity = arity

    def __init__(self, resource_type):
        self._resource_type = resource_type

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
        ident = self.ravel_field_name_transform_inversions.get(ident, ident)
        prop = getattr(self._resource_type, ident)

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
                    ident = self.ravel_field_name_transform_inversions.get(
                        ident, ident
                    )
                    prop = getattr(self._resource_type, ident)
                    value_list = [
                        prop.resolver.field.process(x.strip("'").strip())[0]
                        for x in token.value[1:-1].split(',')
                    ]
                    if is_negative:
                        return prop.excluding(value_list)
                    else:
                        return prop.including(value_list)
