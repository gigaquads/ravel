from collections import defaultdict

from appyratus.enum import EnumValueStr


class Backfill(EnumValueStr):
    @staticmethod
    def values():
        return {'persistent', 'ephemeral'}


class QueryBackfiller(object):
    """
    Storage and logic used during the execution of a Query with backfilling
    enabled.
    """

    def __init__(self):
        self._biz_class_2_objects = defaultdict(list)

    def register(self, obj):
        """
        Recursively extract and store all Resources contained in the given
        `obj` argument. This method is called by the Query object that owns this
        instance and is in the process of executing.
        """
        if is_resource(obj):
            self._biz_class_2_objects[type(obj)].append(obj)
        elif is_batch(obj):
            self._biz_class_2_objects[obj.pybiz.biz_class].extend(obj)
        elif isinstance(value, (list, tuple, set)):
            for item in obj:
                self.register(item)
        elif isinstance(dict):
            for val in obj.values():
                self.register(val)
        else:
            raise Exception('unknown argument type')

    def save(self):
        """
        Save all Resource instances created during the execution of the
        backfilled query utilizing this QueryBackfiller.
        """
        for biz_class, resources in self._biz_class_2_objects.items():
            biz_class.save_many(resources)

    def backfill_query(
        self,
        query: 'Query',
        existing_resources: 'Batch'
    ) -> 'Batch':
        """
        This method is used internally by Query.execute when the Store doesn't
        return a sufficient number of records.
        """
        num_requested = query.params.get('limit', 1)
        num_fetched = len(existing_resources)
        backfill_count = num_requested - num_fetched
        generated_resources = query.generate(count=backfill_count)
        self.register(generated_resources)
        return generated_resources
