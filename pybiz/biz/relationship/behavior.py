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
            'on_add': lambda self, target: behavior.on_add(self, target),
            'on_set': lambda self, target: behavior.on_set(self, target),
            'on_get': lambda self, target: behavior.on_get(self, target),
            'on_rem': lambda self, target: behavior.on_rem(self, target),
            'on_del': lambda self, target: behavior.on_del(self, target),
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
        """
        # Build ID
        """
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
    CRUD Behavior
    """

    @property
    def is_one2one(self):
        return not self._many

    @property
    def is_one2many(self):
        return self._many and len(self._path) == 2

    @property
    def is_many2many(self):
        return self._many and len(self._path) > 2

    def build_conditions(self):
        """
        # Build Conditions
        """
        path = self._path
        behavior = self

        def one2one():
            """
            # One 2 One Relationship Behavior
            Target ID is equal to Source ID
            """
            return lambda self: (
                behavior._target,
                getattr(behavior._target, behavior._target_id)
                == getattr(self, behavior._source_id)
            )

        def one2many():
            """
            # One 2 Many (1..n)
            Target's Source ID is equal to Source ID
            """
            return lambda self: (
                behavior._target,
                getattr(behavior._target, behavior._target_id)
                == getattr(self, behavior._source_id)
            )

        def many2many():
            """
            # Many 2 Many Relationship Behavior (n..n)
            - Bridge's Source ID is equal to Source ID,
            - Bridge List's Target IDs include Target ID
            """
            return (
                lambda self: (
                    behavior._bridge[0],
                    getattr(behavior._bridge[0], behavior._bridge_id[0])
                    == getattr(self, behavior._source_id)
                ),
                lambda bridge_list: (
                    behavior._target,
                    getattr(behavior._target, behavior._target_id)
                    .including(getattr(bridge_list, behavior._bridge_id[1]))
                ),
            )

        if self.is_one2one:
            return one2one()
        elif self.is_one2many:
            return one2many()
        elif self.is_many2many:
            return many2many()

    def on_add(self, source, target):
        """
        # On Add
        The following actions will initiate a call to this method:
        - `BizList.append(BizObject)`
        - `BizList.insert(BizObject)`
        """
        behavior = self
        if not target:
            return

        def one2many():
            """
            # One2Many
            Target contains the foreign key to Source
            """
            return target.merge(
                {
                    behavior._target_id: getattr(
                        source, behavior._source_id
                    )
                }
            ).save()

        def many2many():
            """
            # Many2Many
            Bridge contains both the Source and Target IDs
            """
            return behavior._bridge[0](
                **{
                    behavior._bridge_id[0]: getattr(
                        source, behavior._source_id
                    ),
                    behavior._bridge_id[1]: getattr(
                        target, behavior._target_id
                    ),
                }
            ).save()

        if self.is_one2many:
            return one2many()
        elif self.is_many2many:
            return many2many()

    def on_get(self, source, target):
        """
        # On Get
        The following actions will initiate a call to this method:
        - `print(BizObject.Relationship)`
        """
        behavior = self
        if not target:
            return

        def one2one():
            return getattr(source,
                           behavior._target_id) == getattr(
                               target, behavior._target_id
                           )

        def one2many():
            pass
            # *** AttributeError: 'ProjectBizList' object has no attribute 'project_id'

        def many2many():
            # TODO or not TODO
            pass

        if self.is_one2one:
            return one2one()
        elif self.is_one2many:
            return one2many()
        elif self.is_many2many:
            return many2many()

    def on_set(self, source, target):
        """
        # On Set
        The following actions will initiate a call to this method:
        - `BizObject.Relationship = BizObject`
        """
        behavior = self
        if not target:
            return

        def one2one():
            return (
                getattr(source, behavior._source_id) ==
                getattr(target, behavior._target_id)
            )

        def one2many():
            return getattr(source,
                           behavior._source_id) in getattr(
                               target, behavior._target_id
                           )

        def many2many():
            # XXX How to do this?
            pass

        if self.is_one2one:
            return one2one()
        elif self.is_one2many:
            return one2many()
        elif self.is_many2many:
            return many2many()

    def on_rem(self, source, target):
        """
        # On Remove
        The following actions will initiate a call to this method:
        - `BizList.remove(BizObject)`
        """
        behavior = self
        if not target:
            return

        def one2many():
            """
            # One 2 Many
            """
            return target.merge(
                {
                    behavior._source_id: None
                }
            ).save()

        def many2many():
            """
            # Many 2 Many
            """
            return behavior._bridge[0].query(
                (
                    getattr(
                        behavior._bridge[0],
                        behavior._bridge_id[0]
                    ) == getattr(
                        source, behavior._source_id
                    )
                )
            ).delete()

        if self.is_one2many:
            return one2many()
        elif self.is_many2many:
            return many2many()

    def on_del(self, source, target):
        """
        # On Delete
        The following actions will initiate a call to this method:
        - `BizList.delete()`
        - `del BizObject.Relationship`
        """
        behavior = self
        if not target:
            return

        def one2many():
            return target.merge(
                **{
                    behavior._target_id: None
                }
            ).save()

        def many2many():
            raise NotImplementedError()

        if self.is_one2many:
            return one2many()
        elif self.is_many2many:
            return many2many()
