from typing import (
    List,
    Text,
)

from appyratus.utils import StringUtils
from pybiz.biz.relationship import (
    Relationship,
)
from pybiz.util import is_bizobj


class RelationshipBehavior(object):
    def __init__(self, path: List = None):
        """
        The arity of supplied nodes can vary.  At the very least, one node is
        required to resolve a relationship path.
        - `[User]` - a single node provided, the target biz classassume `_id`
        - `[Group, User]` - two nodes, the source and target biz class
        - `[Group, GroupUser, User]` - three nodes, "bridge"
        """
        self._path = path
        self._source = None
        self._target = None
        self._bridge = None

    def __call__(
        self,
        relationship: 'Relationship',
        many=False,
        *args,
        **kwargs
    ):
        self._relationship = relationship
        self._many = many
        behavior = self
        behaviors = {
            'conditions': behavior.build_conditions(),
            'on_add': lambda self, target: behavior.on_add(self, target, relationship),
            'on_set': lambda self, target: behavior.on_set(self, target, relationship),
            'on_get': lambda self, target: behavior.on_get(self, target, relationship),
            'on_rem': lambda self, target: behavior.on_rem(self, target, relationship),
            'on_del': lambda self, target: behavior.on_del(self, target, relationship),
        }
        return behaviors

    def pre_bootstrap(self):
        self._path = path = self._resolve_path(self._path)

        if len(path) == 1:
            source = self._relationship.biz_type
            self._source = source
            self._source_id = '_id'
            target = path[0]
            self._target = target.target
            self._target_id = target.key
        if len(path) == 2:
            source, target = path
            self._source = source.target
            self._target = target.target
            self._source_id = source.key
            self._target_id = target.key
        elif len(path) == 3:
            source, bridge, target = path
            self._source = source.target
            self._bridge = [b.target for b in bridge]
            self._bridge_id = [b.key for b in bridge]
            self._target = target.target
            self._source_id = source.key
            self._target_id = target.key

    def _resolve_path(self, path: List):
        """
        # Resolve Path
        Path is a list of nodes needed to connect two biz objects

        A node can be provided in many forms
        - `User` - a biz class
        - `User._id` - a field on a biz class
        - `"User"`, `"User._id"` -  a string of a biz class or field, more
          commonly used to avoid circular imports
        - `"GroupUser.group_id:user_id"`, reference multiple fields
        - `[GroupUser.group_id, GroupUser.user_id]`, reference multiple fields
        """
        if not path:
            raise Exception()
        clean_path = []

        def resolve_field(node):
            node_field = None
            # resolve the node class and field from string
            if isinstance(node, str):
                if '.' in node:
                    node_class, node_field = node.split('.')
                node_class = self._relationship.registry.types.biz[
                    node_class]
                if node_field is None:
                    node_field = '_id'
                node_field = getattr(node_class, node_field)
            return node_field

        for node in path:
            if isinstance(node, list):
                node_field = [
                    resolve_field(n) for n in node
                ]
            else:
                node_field = resolve_field(node)
            clean_path.append(node_field)
        return clean_path

    @classmethod
    def _build_id(cls, entity=None) -> Text:
        base_id = '_id'
        if not entity:
            return base_id
        # resolve the entity to name
        if is_bizobj(entity):
            name = entity.__name__
        else:
            name = entity
        # normalize name
        name = StringUtils.snake(name)
        # add base id if not already present
        if name[-3:] == base_id:
            return name
        else:
            return f'{name}{base_id}'


class CrudBehavior(RelationshipBehavior):
    """
    """

    def build_conditions(self):
        """
        # Nodes = 2
        Target ID == Source Target ID

        # Nodes = 3 (bridge)
        Bridge Source ID ==  Source ID
        Bridge List ID includes Target ID
        """
        path = self._path

        def one2one(behavior):
            return lambda self: (
                behavior._target,
                getattr(behavior._target, behavior._build_id())
                == getattr(self, behavior._build_id())
            )

        def one2many(behavior):
            return lambda self: (
                behavior._target,
                getattr(behavior._target, behavior._target_id)
                == getattr(self, behavior._build_id())
            )

        def many2many(behavior):
            return (
                lambda self: (
                    behavior._bridge[0],
                    getattr(behavior._bridge[0], behavior._bridge_id[0])
                    == getattr(self, behavior._build_id())
                ),
                lambda bridge_list: (
                    behavior._target,
                    getattr(behavior._target, behavior._build_id())
                    .including(getattr(bridge_list, behavior._bridge_id[1]))
                ),
            )

        if not self._many:
            return one2one(self)
        else:
            if len(path) == 2:
                return one2many(self)
            else:
                return many2many(self)

    def on_add(self, source, target, relationship):
        #return target.merge(
        #    {
        #        relationship._source_id: source._id
        #    }
        #).save()
        pass

    def on_set(self, source, target, relationship):
        #return source._id in getattr(
        #    target, relationship._source_id
        #)
        pass

    def on_get(self, source, target, relationship):
        pass

    def on_rem(self, source, target, relationship):
        #return target.merge(
        #    {
        #        relationship._source_id: None
        #    }
        #).save()
        pass

    def on_del(self, source, target, relationship):
        pass
