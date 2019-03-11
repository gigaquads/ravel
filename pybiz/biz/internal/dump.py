import copy

from typing import Dict
from collections import defaultdict

from appyratus.utils import StringUtils, DictUtils

from pybiz.util import is_bizobj, is_sequence


class Dumper(object):
    def __call__(self, target: 'BizObject', fields: Dict = None, raw=False) -> Dict:
        # normaize the incoming `fields` data structure to a nested dict
        if isinstance(fields, dict):
            fields = DictUtils.unflatten_keys(fields)
        elif (not fields) or is_sequence(fields):
            fields = DictUtils.unflatten_keys({
                k: None for k in (
                    fields or (target.raw.keys() | target.related.keys())
                )
            })
        else:
            raise ValueError(
                'uncoregnized fields argument type'
            )

        return self.on_dump(target, fields=fields, raw=raw)

    def on_dump(
        self,
        target: 'BizObject',
        fields: Dict = None,
        raw=False,
    ) -> Dict:
        raise NotImplementedError('override in subclass')


class NestingDumper(Dumper):
    def on_dump(
        self,
        target: 'BizObject',
        fields: Dict = None,
        raw=False,
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
        if not raw:
            record = {
                'id': target._id,
                'rev': target._rev,
            }
        else:
            record = {
                '_id': target._id,
                '_rev': target._rev,
            }

        # fields to ignore while dumping, excpecting custom handling
        # in following logic
        pybiz_field_names = {'_id', '_rev'}

        # add each specified field data to the return record
        # and recurse on nested targets available through Relationships
        for k in fields:
            if k in pybiz_field_names:
                continue
            field = target.schema.fields.get(k)
            if field is not None:
                # k corresponds to a field data element
                if not field.meta.get('private', False):
                    # only dump "public" fields
                    v = target.raw[k]
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


class SideLoadingDumper(Dumper):
    def on_dump(
        self,
        target: 'BizObject',
        fields: Dict = None,
        raw=False,
    ):
        raise NotImplementedError('todo')
