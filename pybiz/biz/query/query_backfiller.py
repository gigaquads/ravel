import random

from functools import reduce
from typing import Dict, Text

from appyratus.enum import EnumValueStr

import pybiz.biz

from pybiz.predicate import Predicate


class Backfill(EnumValueStr):
    @staticmethod
    def values():
        return {'persistent', 'ephemeral'}


class QueryBackfiller(object):
    def __init__(self):
        self._biz_class_2_biz_list = {}

    def generate(
        self,
        query: 'Query',
        constraints: Dict[Text, 'Constraint'] = None,
        count: int = None,
    ) -> 'BizList':
        """
        By looking at what fields are being selected int the input query, as
        well as the "where" predicate and other parameters, this method will
        create and return a non-empty BizList belonging to the BizObject class
        targeted by the Query.

        For example, if the `Query` contains a predicate, like `User.age > 50`,
        then all generated User BizObjects will have an randomized age greater
        than 50.

        This method is called during `QueryExecutor.execute` to do the job of
        creating the BizObjects with which we backfill.
        """
        self._biz_class_2_biz_list = {}  # reset storage for generated objects
        targets = self._generate_biz_list(query, count, constraints)
        targets = self._generate_biz_attributes(query, targets)
        return targets

    def persist(self):
        """
        Calling `persist` will call `save` on all BizObjects created by the
        `QueryBackfiller` in the course of executing `generate`.
        """
        for biz_class, biz_list in self._biz_class_2_biz_list.items():
            biz_list.save()

    def _compute_field_value_constraints(self, query, base_constraints):
        """
        In order to generate BizObjects with field values that satisfy the
        conditions specified in the Query's "where" conditions, we derive a set
        of constraints for all fields referenced therein. These constraints are
        interpreted by each Field type when generating a value for it.
        """
        params = query.params

        # base constraints are any that are passed in from outside
        constraints = base_constraints or {}

        # convert and merge the "where" conditions to into
        # the reeturned constraint dict.
        if params.where:
            predicate = Predicate.reduce_and(*params.where)
            constraints.update(predicate.compute_constraints())

        return constraints

    def _generate_biz_list(self, query, count, constraints):
        params = query.params
        biz_class = query.biz_class  # <- the class we are generating
        constraints = self._compute_field_value_constraints(query, constraints)

        # randomize the count if none is explicitly given
        if count is None:
            count_upper_bound = params.limit or random.randint(1, 10)
            count = random.randint(count_upper_bound//2 or 1, count_upper_bound)

        # initialize storage for all instances of the biz_class created
        # in the course of running the self.generate.
        if biz_class not in self._biz_class_2_biz_list:
            self._biz_class_2_biz_list[biz_class] = biz_class.BizList()

        # create the unsaved biz objects
        # and insert into storage dict (_biz_class_2_biz_list) for the sake of
        # possibly saving them via backfiller.persist().
        generated_biz_list = biz_class.BizList.generate(
            count, constraints=constraints
        )

        # apply field property queries (and transforms therein)
        for field_name, fprop_query in query.params.fields.items():
            if fprop_query is not None:
                for biz_obj in generated_biz_list:
                    biz_obj[field_name] = fprop_query.execute(biz_obj)

        self._biz_class_2_biz_list[biz_class].extend(generated_biz_list)

        return generated_biz_list

    def _generate_biz_attributes(
        self, query: 'Query', sources: 'BizList'
    ) -> 'BizList':
        """
        # This requires adding a generate to base BizAttribute
        """
        source_biz_class = query.biz_class
        for name, sub_query in query.params.attributes.items():
            if name not in source_biz_class.pybiz.attributes.relationships:
                # NOTE: relationships are handled elsewhere
                for source in sources:
                    generated_value = sub_query.biz_attr.generate(source)
                    setattr(source, name, generated_value)
        return sources
