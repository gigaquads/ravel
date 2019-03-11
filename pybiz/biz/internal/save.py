from typing import List, Type, Tuple, Dict
from collections import defaultdict

from appyratus.enum import EnumValueStr


class SaveMethod(EnumValueStr):
    @staticmethod
    def values():
        return {
            'breadth-first'
        }


class Saver(object):
    def __init__(self, biz_type: Type['BizObject']):
        self.biz_type = biz_type

    def save_one(self, bizobj) -> 'BizObject':
        raise NotImplementedError('override in subclass')

    def save_many(self, bizobjs: List['BizObject']) -> 'BizList':
        raise NotImplementedError('override in subclass')


class BreadthFirstSaver(Saver):
    def save_one(self, bizobj) -> 'BizObject':
        return self.save_many([bizobj])[0]

    def save_many(self, bizobjs: List['BizObject']) -> 'BizList':
        to_create, to_update = self._aggregate(bizobjs)

        for biz_type, biz_objs in to_create.items():
            if len(biz_objs) == 1:
                biz_objs[0].create()
            else:
                biz_type.create_many(biz_objs)

        for biz_type, biz_objs in to_update.items():
            if len(biz_objs) == 1:
                biz_objs[0].update()
            else:
                biz_type.update_many(biz_objs)

        return self.biz_type.BizList(bizobjs)

    def _aggregate(self, bizobjs: List['BizObject']) -> Tuple[Dict, Dict]:
        to_create = defaultdict(list)
        to_update = defaultdict(list)

        for bizobj in bizobjs:
            if bizobj._id is None:
                to_create[self.biz_type].append(bizobj)
            elif bizobj.dirty:
                to_update[self.biz_type].append(bizobj)
            self._aggregate_related(bizobj, to_create, to_update)

        return (to_create, to_update)

    def _aggregate_related(
        self,
        bizobj: 'BizType',
        to_create: Dict,
        to_update: Dict,
    ) -> None:
        for k, v in bizobj.related.items():
            rel = bizobj.relationships[k]
            if not v:
                continue
            if rel.many:
                for x in v:
                    if x._id is None:
                        to_create[v.biz_type].append(x)
                    elif x.dirty:
                        to_update[v.biz_type].append(x)
                for x in v:
                    self._aggregate_related(x, to_create, to_update)
            else:
                v_type = v.__class__
                if v._id is None:
                    to_create[v_type].append(v)
                elif v.dirty:
                    to_update[v_type].append(v)
                self._aggregate_related(v, to_create, to_update)
