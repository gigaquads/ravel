from typing import Dict, List, Text, Set

from ravel import Resource, Relationship, fields
from ravel.constants import ID, REV

# TODO: implement updates to depth in insert and unlink

class RecursiveList(Resource):
    """
    # RecursiveList
    Each RecursiveList is essentially a linked list, where node that has a
    String name. In addition, each node may have "child" RecursiveLists, in which
    case it is called a "parent" . In other words, this is a recursive data
    structure.

    Let's look at an example. Suppose we have siblings (A, B, C) which are all
    children of X. Then the following statements are true of its relationships:

    ```python
    assert {X} == {A.parent, B.parent, C.parent}
    assert {A, B, C} == set(X.children)
    assert A == B.previous
    assert C == B.following
    ```

    ## Fields
    - `name`: String identifier for this RecursiveList
    - `size`: The number of its children RecursiveLists
    - `position`: The positional index of this RecursiveList in its parent
    """

    '''
    size = fields.Int(nullable=False, default=lambda: 0, required=True)
    depth = fields.Int(nullable=False, default=lambda: 0, required=True)
    position = fields.Int(nullable=False, required=True, default=lambda: -1, private=True)
    parent_id = fields.String(required=True, default=lambda: None, private=True)

    parent = Relationship(
        join=lambda cls: (cls.parent_id, cls._id),
        immutable=True,
    )

    children = Relationship(
        join=lambda cls: (cls._id, cls.parent_id),
        #order_by=lambda cls: cls.position.asc,
        immutable=True,
        many=True,
    )

    previous = Relationship(
        join=lambda cls: cls,
        where=lambda sources: (
            (sources.resource_type.parent_id == sources[0].parent_id) &
            (sources.resource_type.position == sources[0].position - 1)
        ),
        immutable=True,
    )

    following = Relationship(
        join=lambda cls: cls,
        where=lambda sources: (
            (sources.resource_type.parent_id == sources[0].parent_id) &
            (sources.resource_type.position == sources[0].position + 1)
        ),
        immutable=True,
    )


    @classmethod
    def __abstract__(cls):
        return True

    @property
    def has_parent(self):
        return self.parent_id is not None

    def copy(self, fields: Set[Text] = None, recursive=False):
        def copy_one(node, parent_id, fields):
            return self.__class__(
                data={k: node.internal.state[k] for k in fields},
                parent_id=parent_id,
            ).create()

        def copy_children(node, parent_id, fields):
            if not node.size:
                return
            copied_children = self.Batch([])
            node.children.resolve(fields)
            for child in node.children:
                copied_child = copy_one(child, parent_id, fields)
                copied_children.append(copied_child)
            copied_children.create()
            for child, copied_child in zip(node.children, copied_children):
                copy_children(child, copied_child._id, fields)

        # copy all non-internal fields by default
        fields = (
            set(fields or self.schema.fields.keys())
                - {ID, REV}
        )

        # copy this instance & conditionally children, recursively
        copied = copy_one(self, self.parent_id, fields)
        if recursive:
            copy_children(self, copied._id, fields)

        return copied

    def unlink(self):
        """
        Disassociate this RecursiveList with its parent, adjusting the indexes
        of all sibling RecursiveLists that remain.
        """
        if self.parent_id is None:
            return

        dirty = self.Batch([self, self.parent])  # resources to update

        # adjust remaining sibling's positions
        siblings = self.query(
            (self.__class__.parent_id == self._id) &
            (self.__class__.position > self.position)
        )
        if siblings:
            dirty.extend(siblings)
            for sibling in siblings:
                sibling.position -= 1

        self.position = -1
        self.parent_id = None
        self.parent.size -= 1
        self.unload({'parent', 'previous', 'following'})
        dirty.update()

    def insert_many(self, children: List['RecursiveList'], index: int, copy=False):
        """
        Remove each child RecursiveList from whatever list it may already belong to
        and insert it in this RecursiveList, adjusting the position of all sibling
        RecursiveLists downstream.
        """
        position = max(0, min(self.size, index))
        do_shift_sibling_positions = position < self.size

        dirty = self.Batch([self])  # resources to update

        for j, child in enumerate(children):
            if copy:
                child = child.copy(recursive=True)
            elif child.has_parent:
                child.unlink()
            child.unload({'parent', 'following', 'previous'})
            child.parent_id = self._id
            child.position = position + j
            dirty.append(child)
            self.size += 1

        if do_shift_sibling_positions:
            siblings = self.query(
                predicate=(
                    (self.__class__.parent_id == self._id) &
                    (self.__class__.position >= position)
                ),
                order_by=self.__class__.position.asc
            )
            if siblings:
                dirty.extend(siblings)
                offset = position + len(children)
                for i, sibling in enumerate(siblings):
                    sibling.position = offset + i

        self.unload('children')
        dirty.update()

    def insert(self, child: 'RecursiveList', index: int, copy=False):
        """
        Insert a RecursiveList at the given index.
        """
        self.insert_many([child], index, copy=copy)

    def unshift(self, child: 'RecursiveList', copy=False):
        """
        Insert a child RecursiveList at the head of this RecursiveList's children.
        """
        self.insert_many([child], 0, copy=copy)

    def unshift_many(self, children: List['RecursiveList'], copy=False):
        """
        Insert a list of RecursiveLists at the head of this RecursiveList's children.
        """
        self.insert_many(children, 0, copy=copy)

    def push(self, child: 'RecursiveList', copy=False):
        """
        Insert a child RecursiveList at the tail of this RecursiveList's children.
        """
        self.insert_many([child], self.size, copy=copy)

    def push_many(self, children: List['RecursiveList'], copy=False):
        """
        Insert a list of RecursiveLists at the tail of this RecursiveList's children.
        """
        self.insert_many(children, self.size, copy=copy)

    def reverse(self):
        for i, child in enumerate(self.children):
            child.position = len(self.children) - i - 1
            child.unload({'following', 'previous'})
        self.unload('children')
        self.children.update()

    def apply(self, func, direction, args=None, kwargs=None, inclusive=True):
        args = args or tuple()
        kwargs = kwargs or {}
        results = []
        if inclusive:
            result = func(self, *args, **kwargs)
            results.append(result)
        if direction < 0:
            node = self.r.parent.query()
            while node is not None:
                result = func(node, *args, **kwargs)
                results.append(result)
                node = node.parent
        else:
            for node in self.r.children.query():
                child_results = node.apply(func, direction, args, kwargs)
                results.extend(child_results)
        return results

'''












if __name__ == '__main__':
    from ravel.app import ReplApplication

    repl = ReplApplication()

    class RecursiveList(RecursiveList):
        name = fields.String()

    @repl()
    def test_push():
        parent = RecursiveList(name='parent', position=0).save()
        for i in range(3):
            child = RecursiveList(name=f'child_{i}').create()
            parent.push(child)

        for s1, s2 in zip(parent.children, parent.children[1:]):
            assert s1.position == s2.position - 1
            assert int(s1.name[-1]) == int(s2.name[-1]) - 1

        repl.namespace.update(locals())

    @repl()
    def test_unshift():
        parent = RecursiveList(name='parent', position=0).save()
        for i in range(3):
            child = RecursiveList(name=f'child_{i}').create()
            parent.unshift(child)

        for s1, s2 in zip(parent.children, parent.children[1:]):
            assert s1.position == s2.position - 1
            assert int(s1.name[-1]) == int(s2.name[-1]) + 1

        repl.namespace.update(locals())

    @repl()
    def test_unlink():
        parent = RecursiveList(name='parent', position=0).save()
        child = RecursiveList(name='child').save()

        parent.unshift(child)

        assert parent.size == 1
        assert parent._id == child.parent_id
        assert child.position == 0

        child.unlink()

        assert parent.size == 0
        assert child.parent_id is None
        assert child.position == -1

        repl.namespace.update(locals())

    @repl()
    def test_reverse():
        parent = RecursiveList(name='parent', position=0).save()
        for i in range(3):
            child = RecursiveList(name=f'child_{i}').create()
            parent.push(child)

        parent.reverse()

        for s1, s2 in zip(parent.children, parent.children[1:]):
            assert int(s1.name[-1]) == int(s2.name[-1]) - 1

        repl.namespace.update(locals())

    @repl()
    def test_insert_many():
        parent = RecursiveList(name='parent', position=0).save()
        children = [
            RecursiveList(name=f'child_{i}').create() for i in range(1, 6)
        ]
        for child in children[:2]:
            parent.push(child)

        parent.insert_many(children[2:], 1, copy=True)

        for s1, s2 in zip(parent.children[1:], parent.children[2:]):
            assert s1.position == s2.position - 1
            if s2 is not parent.children[-1]:
                assert int(s1.name[-1]) == int(s2.name[-1]) - 1

        assert parent.children[-1].name == 'child_2'

        repl.namespace.update(locals())


    repl.bootstrap(namespace=globals()).start()
