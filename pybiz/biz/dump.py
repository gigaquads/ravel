import copy

from typing import Dict, Text
from collections import defaultdict

from appyratus.utils import StringUtils, DictUtils

from pybiz.util.misc_functions import is_bizobj, is_bizlist, is_sequence


class Dumper(object):
    def __call__(self, target: 'BizObject', fields: Dict = None) -> Dict:
        # normaize the incoming `fields` data structure to a nested dict
        if isinstance(fields, dict):
            fields = DictUtils.unflatten_keys(fields)
        elif (not fields) or is_sequence(fields):
            if is_bizobj(target):
                fields = DictUtils.unflatten_keys({
                    k: None for k in (
                        fields or (
                            target.internal.state.keys()
                            | target.internal.attributes.keys()
                        )
                    )
                })
            elif is_sequence(target) or is_bizlist(target):
                fields = DictUtils.unflatten_keys({
                    k: None for k in (
                        fields or (
                            target.internal.state.keys()
                            | target.internal.attributes.keys()
                        )
                    )
                })

        else:
            raise ValueError(
                'uncoregnized fields argument type'
            )

        return self.on_dump(target, fields=fields)

    def on_dump(self, target: 'BizObject', fields: Dict = None) -> Dict:
        raise NotImplementedError('override in subclass')


class NestingDumper(Dumper):
    def on_dump(
        self,
        target: 'BizObject',
        fields: Dict[Text, Dict] = None,
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
        # fields to ignore while dumping, excpecting custom handling
        fields_to_ignore = {'_id', '_rev'}

        # `record` is the return values
        record = {
            'id': target._id,
            'rev': target._rev,
        }

        # add each specified field data to the return record
        # and recurse on nested targets available through Relationships
        for k in fields:
            if k in fields_to_ignore:
                continue

            field = target.schema.fields.get(k)
            if field is not None:
                # k corresponds to a field data element
                if not field.meta.get('private', False):
                    # only dump "public" fields
                    v = target.internal.state[k]
                    if is_sequence(v):
                        v = copy.deepcopy(v)
                    record[k] = v
            elif k in target.relationships:
                rel = target.relationships[k]
                if rel.private:
                    continue
                # k corresponds to a declared Relationship, which could
                # refer either to an instance object or a list thereof.
                related = getattr(target, k, None)
                if related is None:
                    record[k] = None
                elif is_bizobj(related):
                    record[k] = self(related, fields=fields[k])
                else:
                    record[k] = [
                        self(obj, fields=fields[k])
                        for obj in related
                    ]
            elif k in target.attributes:
                biz_attr = target.attributes.by_name(k)
                if not biz_attr.private:
                    value = getattr(target, k, None)
                    record[k] = self._dump_object(value)

        return record

    def _dump_object(self, obj):
        if is_bizobj(obj) or is_bizlist(obj):
            return obj.dump()
        elif is_sequence(obj):
            return [
                self._dump_object(x) for x in obj
            ]
        elif isinstance(obj, dict):
            return {
                self._dump_object(k): self._dump_object(v)
                for k, v in obj.items()
            }
        return obj


class SideLoadingDumper(Dumper):
    def on_dump(
        self,
        target: 'BizObject',
        fields: Dict = None,
        raw=False,
    ):
        raise NotImplementedError('todo')
