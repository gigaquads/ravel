import copy

from typing import Dict
from collections import defaultdict

from appyratus.utils import StringUtils, DictUtils

from pybiz.util import is_bizobj


class Dump(object):
    def __call__(self, target: 'BizObject', fields: Dict = None) -> Dict:
        # normaize the incoming `fields` data structure to a nested dict
        if isinstance(fields, dict):
            fields = DictUtils.unflatten_keys(fields)
        elif (not fields) or isinstance(fields, (set, list, tuple)):
            fields = DictUtils.unflatten_keys({
                k: None for k in (
                    fields or (target.data.keys() | target.related.keys())
                )
            })
        else:
            raise ValueError(
                'uncoregnized fields argument type'
            )

        return self.on_dump(target, fields=fields, parent=None)

    def on_dump(
        self,
        target: 'BizObject',
        parent: 'BizObject' = None,
        fields: Dict = None,
    ) -> Dict:
        raise NotImplementedError('override in subclass')


class DumpNested(Dump):
    def on_dump(
        self,
        target: 'BizObject',
        parent: 'BizObject' = None,
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
        record = {}  # `record` is the return values

        # normalize fields objec to aict. if empty, default
        # to all loaded data and relationships
        if isinstance(fields, dict):
            fields = DictUtils.unflatten_keys(fields)
        elif (not fields) or isinstance(fields, (set, list, tuple)):
            if target is None:
                import ipdb; ipdb.set_trace()
            fields = DictUtils.unflatten_keys({
                k: None for k in (
                    fields or (target.data.keys() | target.related.keys())
                )
            })
        else:
            raise ValueError(
                'uncoregnized fields argument type'
            )

        # ensure _id is always added as "id"
        record['id'] = target._id

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
                    if isinstance(v, (dict, list, set, tuple)):
                        v = copy.deeepcopy(v)
                    record[k] = v
            elif k in target.relationships:
                # k corresponds to a declared Relationship, which could
                # refer either to an instance object or a list thereof.
                related = target.related[k]
                if is_bizobj(related):
                    record[k] = self.on_dump(
                        related, fields=fields[k], parent=target
                    )
                else:
                    record[k] = [
                        self.on_dump(obj, parent=target) for obj in related
                    ]

        return record


class DumpSideLoaded(Dump):
    def on_dump(
        self,
        target: 'BizObject',
        fields: Dict = None,
        parent: 'BizObject' = None,
    ):
        raise NotImplementedError()
