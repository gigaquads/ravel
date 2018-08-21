class Predicate(object):
    pass


class ConditionalPredicate(Predicate):
    def __init__(self, attr_name, op, value):
        self.attr_name = attr_name
        self.op = op
        self.value = value

    def __repr__(self):
        return '<{}({} {} {})>'.format(
            self.__class__.__name__,
            self.attr_name,
            self.op,
            self.value,
        )

    def __or__(self, other):
        return BooleanPredicate('|', self, other)

    def __and__(self, other):
        return BooleanPredicate('&', self, other)

    @property
    def display_string(self):
        return '({} {} {})'.format(
            self.attr_name, self.op, self.value
        )


class BooleanPredicate(Predicate):
    def __init__(self, op, lhs, rhs=None):
        self.op = op
        self.lhs = lhs
        self.rhs = rhs

    def __repr__(self):
        return '<{}({} {} {})>'.format(
            self.__class__.__name__,
            self.lhs,
            self.op,
            self.rhs,
        )
    def __or__(self, other):
        return BooleanPredicate('|', self, other)

    def __and__(self, other):
        return BooleanPredicate('&', self, other)


    @property
    def display_string(self):
        def recurse(p):
            substr = ''
            if isinstance(p.lhs, BooleanPredicate):
                substr += recurse(p.lhs)
            else:
                substr += p.lhs.display_string
            substr += ' {} '.format(p.op)
            if isinstance(p.rhs, BooleanPredicate):
                substr += recurse(p.rhs)
            else:
                substr += p.rhs.display_string
            return '(' + substr + ')'
        return recurse(self)
