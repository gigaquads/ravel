from typing import Text, Set, Dict

from appyratus.enum import EnumValueStr

from pybiz.constants import (
    ID_FIELD_NAME,
)

from .util import is_biz_list, is_biz_object


class DumpStyle(EnumValueStr):
    @staticmethod
    def values():
        return {
            'nested',
            'side_loaded',
        }


class Dumper(object):

    @classmethod
    def get_style(cls) -> DumpStyle:
        raise NotImplementedError()

    @classmethod
    def for_style(cls, style: DumpStyle) -> 'Dumper':
        if style == DumpStyle.nested:
            return NestedDumper()
        if style == DumpStyle.side_loaded:
            return SideLoadedDumper()


class NestedDumper(Dumper):

    @classmethod
    def get_style(cls) -> DumpStyle:
        return DumpStyle.nested

    def dump(self, target: 'BizObject', keys: Set = None) -> Dict:
        return self._dump_recursive(target, keys)

    def _dump_recursive(
        self, parent_biz_object: 'BizObject', keys: Set
    ) -> Dict:

        if keys:
            keys_to_dump = keys if isinstance(keys, set) else set(keys)
        else:
            keys_to_dump = parent_biz_object.internal.state.keys()

        record = {}
        for k in keys_to_dump:
            v = parent_biz_object.internal.state.get(k)
            resolver = parent_biz_object.internal.resolvers.get(k)
            if resolver is None:
                resolver = parent_biz_object.pybiz.resolvers.get(k)
            assert resolver is not None
            if k in parent_biz_object.pybiz.resolvers.relationships:
                # handle the dumping of Relationships specially
                rel = resolver
                if rel.many:
                    assert is_biz_list(v)
                    child_biz_list = v
                    record[k] = [
                        self.dump(child_biz_obj)
                        for child_biz_obj in child_biz_list
                    ]
                else:
                    if v is not None:
                        assert is_biz_object(v)
                    child_biz_obj = v
                    record[k] = self.dump(child_biz_obj)
            else:
                # dump non-Relationship state
                record[k] = resolver.dump(self, v)


        return record


class SideLoadedDumper(Dumper):

    @classmethod
    def get_style(cls) -> DumpStyle:
        return DumpStyle.side_loaded

    def dump(self, target: 'BizObject', keys: Set = None) -> Dict:
        links = self._dump_recursive(target)
        return {
            'target': links.pop(target._id),
            'links': links,
        }

    def _dump_recursive(
        self, parent_biz_object: 'BizObject', links: Dict = None
    ):
        links = links if links is not None else {}
        relationships = parent_biz_object.pybiz.resolvers.relationships

        record = {}
        for k, v in parent_biz_object.internal.state.items():
            resolver = parent_biz_object.pybiz.resolvers[k]
            if resolver.name in relationships:
                relationship = resolver
                record[k] = getattr(v, ID_FIELD_NAME)
                if k in parent_biz_object.internal.state:
                    self._recurse_on_biz_thing(v, links)
            else:
                record[k] = resolver.dump(self, v)

        parent_id = getattr(parent_biz_object, ID_FIELD_NAME)

        if parent_id not in links:
            links[parent_id] = record
        else:
            links[parent_id].update(record)

        return links

    def _recurse_on_biz_thing(self, biz_thing: 'BizThing', links: Dict):
        if is_biz_list(biz_thing):
            rel_biz_objects = biz_thing
        else:
            rel_biz_objects = [biz_thing]
        for rel_biz_obj in rel_biz_objects:
            self._dump_recursive(rel_biz_obj, links)

    def _extract_relationship_state(self, biz_object: 'BizObject') -> Dict:
        return {
            k: biz_object.internal.state[k]
            for k in biz_object.pybiz.resolvers.relationships
            if k in biz_object.internal.state
        }
