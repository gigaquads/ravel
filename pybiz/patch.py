from collections import defaultdict

from . import util

from .const import (
    ROOT_ATTR,
    PATCH_PATH_ANNOTATION,
    PRE_PATCH_ANNOTATION,
    POST_PATCH_ANNOTATION,
    PATCH_ANNOTATION,
    )


class JsonPatchMixin(object):

    _patch_map = defaultdict(lambda: defaultdict(str))
    """Format: `{class_name: {path_str: cb_method_name}`"""

    _pre_patch_map = defaultdict(lambda: defaultdict(list))
    """Format: `{class_name: {path_str: [cb_method_name]}`"""

    _post_patch_map = defaultdict(lambda: defaultdict(list))
    """Format: `{class_name: {path_str: [cb_method_name]}`"""

    def patch(self, op, path, value=None):
        ctx = self._build_patch_context(path)
        cancel_propagation = False
        for idx, biz_obj in reversed(ctx['biz_objs']):
            rel_path = self._build_relative_path(ctx, idx)
            try:
                self._apply_pre_patch_hooks(ctx, op, rel_path, value)
                self._apply_patch_delta(idx, biz_obj, ctx, op, rel_path, value)
                self._apply_post_patch_hooks(ctx, op, rel_path, value)
            except JsonPatchStop:
                cancel_propagation = True
            if cancel_propagation:
                break

    @staticmethod
    def _build_relative_path(ctx, idx):
        return [
            JsonPatchPathComponent(k, v) for (k, v) in
                zip(ctx['tokenized_path'][idx:],
                    ctx['objs'][idx:])
            ]

    def _apply_patch_delta(self, idx, biz_obj, ctx, op, rel_path, value):
        custom_apply_delta = self.get_patch_hook(ctx['path'])
        if custom_apply_delta:
            custom_apply_delta(op, rel_path, value)
        else:
            biz_obj.apply_delta(op, rel_path, value)

        # add the bizobj field that contains the patched data
        # to the dirty set.
        biz_obj.mark_dirty(ctx['tokenized_path'][idx+1])

    def _apply_pre_patch_hooks(self, ctx, op, rel_path, value):
        for k in self.get_pre_patch_hooks(ctx['path']):
            getattr(self, k)(op, rel_path, value)

    def _apply_post_patch_hooks(self, ctx, op, rel_path, value):
        for k in self.get_post_patch_hooks(ctx['path']):
            getattr(self, k)(op, rel_path, value)

    @classmethod
    def get_pre_patch_hooks(cls, path):
        class_name = cls.__name__
        return cls._pre_patch_map[class_name][path]

    @classmethod
    def add_pre_patch_hook(cls, path, method_name):
        class_name = cls.__name__
        assert hasattr(cls, method_name)
        cls._pre_patch_map[class_name][path].append(method_name)

    @classmethod
    def get_post_patch_hooks(cls, path):
        class_name = cls.__name__
        return cls._post_patch_map[class_name][path]

    @classmethod
    def add_post_patch_hook(cls, path, method_name):
        class_name = cls.__name__
        assert hasattr(cls, method_name)
        cls._post_patch_map[class_name][path].append(method_name)

    @classmethod
    def get_patch_hook(cls, path):
        class_name = cls.__name__
        return cls._patch_map[class_name][path]

    @classmethod
    def set_patch_hook(cls, path, method_name):
        class_name = cls.__name__
        assert not cls._patch_map[class_name][path]
        cls._patch_map[class_name][path] = method_name

    def apply_delta(self, op, path, value=None):
        assert len(path) >= 2
        obj = path[-2].obj
        sub_obj = path[-1].obj
        attr = path[-1].parent_attr
        if op == 'remove':
            if attr.isdigit():
                assert isinstance(obj, list)
                idx = int(attr)
                new_obj = obj[:idx] + obj[idx+1:]
                obj.clear()
                obj.extend(new_obj)
            else:
                obj[attr] = None
        elif op in ('add', 'replace'):  # break this apart
            is_list = isinstance(sub_obj, list)
            if op == 'add':
                if is_list:
                    sub_obj.append(value)
                else:
                    is_bizobj = util.is_bizobj(sub_obj)
                    is_dict = isinstance(sub_obj, dict)
                    if is_dict or is_bizobj:
                        assert attr not in sub_obj
                        sub_obj[attr] = value
            elif op == 'replace':
                if is_list:
                    idx_str = attr
                    assert attr
                    assert isinstance(idx_str, str)
                    assert idx_str.isdigit()
                    idx = int(idx_str)
                    assert 0 <= idx
                    assert idx < len(obj)
                    sub_obj[idx] = value
                else:
                    assert attr in obj
                    obj[attr] = value

    def _build_patch_context(self, path: str):
        tokenized_path = self._parse_path(path)
        obj = self
        objs = [obj]
        biz_objs = [(0, obj)]
        for i, token in enumerate(tokenized_path):
            if token == ROOT_ATTR:
                continue
            is_bizobj = util.is_bizobj(obj)
            if is_bizobj:
                sub_obj = getattr(obj, token, None)
            elif token.isdigit():
                #raise NotImplementedError('list deltas are not implemented')
                sub_obj = obj[int(token)]
            else:
                assert isinstance(obj, dict)
                sub_obj = obj[token]
            assert sub_obj is not None
            objs.append(sub_obj)
            is_sub_bizobj = getattr(sub_obj, 'is_bizobj', False)
            if is_sub_bizobj:
                biz_objs.append((i, sub_obj))
            obj = sub_obj
        return {
            'path': path,
            'tokenized_path': tokenized_path,
            'biz_objs': biz_objs,
            'objs': objs,
            }

    def _parse_path(self, path:str):
        path_crumbs = [ROOT_ATTR]
        path_crumbs.extend(path.strip('/').split('/'))
        return path_crumbs


class JsonPatchPathComponent(object):
    def __init__(self, parent_attr, obj):
        self._item = (parent_attr, obj)

    def __repr__(self):
        return str(self._item)

    @property
    def parent_attr(self):
        return self._item[0]

    @property
    def obj(self):
        return self._item[1]


class JsonPatchStop(Exception):
    pass


class post_patch(object):
    def __init__(self, path):
        self.path = path

    def __call__(self, func):
        func._post_patch = True
        func._patch_path = self.path
        return func


class pre_patch(object):
    def __init__(self, path):
        self.path = path

    def __call__(self, func):
        func._pre_patch = True
        func._patch_path = self.path
        return func


class patch(object):
    def __init__(self, path):
        self.path = path

    def __call__(self, func):
        func._patch = True
        func._patch_path = self.path
        return func
