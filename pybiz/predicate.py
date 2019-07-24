import pickle
import codecs

from collections import defaultdict
from typing import Text, Type, Dict

from appyratus.enum import Enum


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
    OP_CODE.INCLUDING: 'IN',
    OP_CODE.EXCLUDING: 'NOT IN',
    OP_CODE.AND: 'AND',
    OP_CODE.OR: 'OR',
}

TYPE_BOOLEAN = 1
TYPE_CONDITIONAL = 2


class Predicate(object):
    TYPE_BOOLEAN = TYPE_BOOLEAN
    TYPE_CONDITIONAL = TYPE_CONDITIONAL

    def __init__(self, code):
        self.code = code

    def serialize(self) -> Text:
        pickled = codecs.encode(pickle.dumps(self), "base64").decode()
        return '#' + pickled + '#'

    @staticmethod
    def deserialize(obj) -> 'Predicate':
        if isinstance(obj, str) and obj[0] == '#' and obj[-1] == '#':
            return pickle.loads(codecs.decode(obj[1:-1].encode(), "base64"))
        elif isinstance(obj, Predicate):
            return obj
        else:
            raise ValueError(str(obj))

    def dump(self):
        raise NotImplementedError()

    @classmethod
    def load(cls, biz_type: Type['BizObject'], data: Dict):
        if data['code'] == TYPE_CONDITIONAL:
            return ConditionalPredicate.load(biz_type, data)
        elif data['code'] == TYPE_BOOLEAN:
            return BooleanPredicate.load(biz_type, data)


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

    def __repr__(self):
        return '<{}({})>'.format(
            self.__class__.__name__,
            str(self)[1:-1],
        )

    def __str__(self):
        if self.prop:
            host_name = self.prop.target.__name__
            lhs = host_name + '.' + self.prop.key
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

    @property
    def targets(self):
        return [self.prop.target]

    def dump(self):
        return {
            'op': self.op,
            'field': self.prop.field.name,
            'value': self.value,
            'code': self.code,
        }

    @classmethod
    def load(cls, biz_type: Type['BizObject'], data: Dict):
        field_prop = getattr(biz_type, data['field'])
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

        # collect all BizObject classes whose fields are involved in the
        # contained predicates. These classes are referred to as the "target"
        # classes, in this context.
        self.targets = set()
        for predicate in [lhs, rhs]:
            if predicate:
                self.targets.update(predicate.targets)
        self.targets = list(self.targets)

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
    def load(cls, biz_type: Type['BizObject'], data: Dict):
        return cls(
            data['op'],
            Predicate.load(biz_type, data['lhs']),
            Predicate.load(biz_type, data['rhs']),
        )
