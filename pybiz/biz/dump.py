import copy

from typing import Dict
from collections import defaultdict

from appyratus.utils import StringUtils, DictUtils

from pybiz.util import is_bizobj, is_sequence


class Dump(object):
    def __call__(self, target: 'BizObject', fields: Dict = None) -> Dict:
        # normaize the incoming `fields` data structure to a nested dict
        if isinstance(fields, dict):
            fields = DictUtils.unflatten_keys(fields)
        elif (not fields) or is_sequence(fields):
            fields = DictUtils.unflatten_keys({
                k: None for k in (
                    fields or (target.data.keys() | target.related.keys())
                )
            })
        else:
            raise ValueError(
                'uncoregnized fields argument type'
            )

        return self.on_dump(target, fields=fields)

    def on_dump(
        self,
        target: 'BizObject',
        fields: Dict = None,
    ) -> Dict:
        raise NotImplementedError('override in subclass')


class DumpNested(Dump):
    def on_dump(
        self,
        target: 'BizObject',
        fields: Dict = None,
    ):
        """
        Dump the target BizObject as a nested dictionary. For example, imagine
        you have a user biz object with an associated account object, declared
        as a `Relationship`. You can do,

        ```python3
            dump(user, {'email', 'account.name'}) -> {
                'id': 1,
                'email': 'foo@bar.com',
                'account': {'id': 2, 'name': 'Foo Co.'}
            }
        ```

        The fields can be specified either as a set (as seen above) or as
        a nested `dict` with `None` as a terminator, like:

        ```python3
            dump(user, {'email', 'account': {'name': None}}) -> {
                'id': 1,
                'email': 'foo@bar.com',
                'account': {'id': 2, 'name': 'Foo Co.'}
            }
        ```
        """
        # `record` is the return values
        record = {'id': target._id}

        # add each specified field data to the return record
        # and recurse on nested targets available through Relationships
        for k in fields:
            if k == '_id':
                continue
            field = target.schema.fields.get(k)
            if field is not None:
                # k corresponds to a field data element
                if not field.meta.get('private', False):
                    # only dump "public" fields
                    v = target.data[k]
                    if is_sequence(v):
                        v = copy.deepcopy(v)
                    record[k] = v
            elif k in target.relationships:
                rel = target.relationships[k]
                if rel.private:
                    continue
                # k corresponds to a declared Relationship, which could
                # refer either to an instance object or a list thereof.
                related = target.related[k]
                if related is None:
                    record[k] = None
                elif is_bizobj(related):
                    record[k] = self(related, fields=fields[k])
                else:
                    record[k] = [
                        self(obj, fields=fields[k])
                        for obj in related
                    ]

        return record


class DumpSideLoaded(Dump):
    def on_dump(
        self,
        target: 'BizObject',
        fields: Dict = None,
    ):
        raise NotImplementedError()
