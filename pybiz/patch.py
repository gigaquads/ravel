from collections import defaultdict

from . import util

from .const import (
    ROOT_ATTR,
    PATCH_PATH_ANNOTATION,
    PRE_PATCH_ANNOTATION,
    POST_PATCH_ANNOTATION,
    PATCH_ANNOTATION,
    OP_DELTA_REMOVE,
    OP_DELTA_ADD,
    OP_DELTA_REPLACE,
    RECOGNIZED_DELTA_OPS,
    )

# TODO: write JsonPatch unit test

class JsonPatchMixin(object):

    # Format: `{class_name: {path_str: cb_method_name}`
    _patch_map = defaultdict(lambda: defaultdict(str))

    # Format: `{class_name: {path_str: [cb_method_name]}`
    _pre_patch_map = defaultdict(lambda: defaultdict(list))

    # Format: `{class_name: {path_str: [cb_method_name]}`
    _post_patch_map = defaultdict(lambda: defaultdict(list))

    def patch(self, op, path, value=None):
        ctx = self._build_patch_context(path)
        cancel_propagation = False
        for idx, biz_obj in reversed(ctx['biz_objs']):
            rel_path = self._build_relative_path(ctx, idx)
            try:
                self._apply_pre_patch_hooks(ctx, op, rel_path, value)
                self._apply_patch_delta(idx, biz_obj, ctx, op, rel_path, value)
                self._apply_post_patch_hooks(ctx, op, rel_path, value)
            except JsonPatchStopSignal:
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
        # XXX: remvove this when we're sure that the DirtyDict
        # is already marking patched fields as dirty
        # biz_obj.mark_dirty(ctx['tokenized_path'][idx+1])

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
        assert op in RECOGNIZED_DELTA_OPS
        assert len(path) >= 2

        obj = path[-2].obj
        sub_obj = path[-1].obj
        key = path[-1].key_in_parent

        if op == OP_DELTA_REMOVE:
            if key.isdigit():
                assert isinstance(obj, list)
                idx = int(key)
                new_obj = obj[:idx] + obj[idx+1:]
                obj.clear()
                obj.extend(new_obj)
            else:
                obj[key] = None
            return

        is_list = isinstance(sub_obj, list)

        if op == OP_DELTA_ADD:
            if is_list:
                sub_obj.append(value)
            else:
                is_bizobj = util.is_bizobj(sub_obj)
                is_dict = isinstance(sub_obj, dict)
                if is_dict or is_bizobj:
                    assert key not in sub_obj
                    sub_obj[key] = value
        elif op == OP_DELTA_REPLACE:
            if is_list:
                assert key
                idx_str = key
                assert isinstance(idx_str, str)
                assert idx_str.isdigit()
                idx = int(idx_str)
                assert 0 <= idx
                assert idx < len(obj)
                sub_obj[idx] = value
            else:
                assert key in obj
                obj[key] = value

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
                sub_obj = obj[int(token)]
            else:
                assert isinstance(obj, dict)
                sub_obj = obj[token]

            assert sub_obj is not None
            objs.append(sub_obj)

            if util.is_bizobj(sub_obj):
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
    def __init__(self, key_in_parent, obj):
        self._item = (key_in_parent, obj)

    def __repr__(self):
        return str(self._item)

    @property
    def key_in_parent(self):
        return self._item[0]

    @property
    def obj(self):
        return self._item[1]


class JsonPatchStopSignal(Exception):
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
