from typing import Text, Set, Dict

from appyratus.enum import EnumValueStr

from ravel.constants import (
    ID,
    REV
)

from ravel.util import is_batch, is_resource


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

    def dump(self, target: 'Resource', keys: Set = None) -> Dict:
        return self._dump_recursive(target, keys)

    def _dump_recursive(
        self, parent_resource: 'Resource', keys: Set
    ) -> Dict:

        if parent_resource is None:
            return None

        if keys:
            keys_to_dump = keys if isinstance(keys, set) else set(keys)
        else:
            keys_to_dump = parent_resource.internal.state.keys()

        record = {}
        for k in keys_to_dump:
            v = parent_resource.internal.state.get(k)
            resolver = parent_resource.ravel.resolvers.get(k)
            assert resolver is not None

            if resolver.private:
                continue

            if k in parent_resource.ravel.resolvers.relationships:
                # handle the dumping of Relationships specially
                rel = resolver
                if rel.many:
                    assert is_batch(v)
                    child_batch = v
                    record[k] = [
                        self.dump(child_resource)
                        for child_resource in child_batch
                    ]
                else:
                    if v is not None:
                        assert is_resource(v)
                    child_resource = v
                    record[k] = self.dump(child_resource)
            else:
                # dump non-Relationship state
                if k in {ID, REV}:
                    k = k[1:]
                record[k] = resolver.dump(self, v)

        return record


class SideLoadedDumper(Dumper):

    @classmethod
    def get_style(cls) -> DumpStyle:
        return DumpStyle.side_loaded

    def dump(self, target: 'Resource', keys: Set = None) -> Dict:
        links = self._dump_recursive(target)
        return {
            'target': links.pop(target._id),
            'links': links,
        }

    def _dump_recursive(
        self, parent_resource: 'Resource', links: Dict = None
    ):
        links = links if links is not None else {}
        relationships = parent_resource.ravel.resolvers.relationships

        record = {}
        for k, v in parent_resource.internal.state.items():
            if k in {ID, REV}:
                k = k[1:]

            resolver = parent_resource.ravel.resolvers[k]
            if resolver.name in relationships:
                relationship = resolver
                record[k] = getattr(v, ID)
                if k in parent_resource.internal.state:
                    self._recurse_on_entity(v, links)
            else:
                record[k] = resolver.dump(self, v)

        parent_id = getattr(parent_resource, ID)

        if parent_id not in links:
            links[parent_id] = record
        else:
            links[parent_id].update(record)

        return links

    def _recurse_on_entity(self, entity: 'Entity', links: Dict):
        if is_batch(entity):
            rel_resources = entity
        else:
            rel_resources = [entity]
        for rel_resource in rel_resources:
            self._dump_recursive(rel_resource, links)

    def _extract_relationship_state(self, resource: 'Resource') -> Dict:
        return {
            k: resource.internal.state[k]
            for k in resource.ravel.resolvers.relationships
            if k in resource.internal.state
        }
