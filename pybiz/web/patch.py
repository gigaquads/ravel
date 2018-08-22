from collections import defaultdict

from pybiz.util import is_bizobj

from .constants import (
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

# TODO: write JsonPatch patch method test
# TODO: write JsonPatch hook method tests


class JsonPatchMixin(object):

    # Format: `{class_name: {path_str: cb_method_name}`
    _patch_map = defaultdict(lambda: defaultdict(str))

    # Format: `{class_name: {path_str: [cb_method_name]}`
    _pre_patch_map = defaultdict(lambda: defaultdict(list))

    # Format: `{class_name: {path_str: [cb_method_name]}`
    _post_patch_map = defaultdict(lambda: defaultdict(list))

    def patch(self, op: str, path: str, value=None):
        ctx = self._build_patch_context(op, path, value)
        cancel_propagation = False
        for (i, (idx, bizobj)) in enumerate(reversed(ctx['bizobjs'])):
            # avoid trying to patch a bizobj that we simply want to remove or
            # replace from its parent object or list:
            if (op != OP_DELTA_ADD) and (len(ctx['tokenized_path'][idx:]) == 1):
                continue

            relative_path = self._build_relative_path(ctx, idx)

            try:
                self._apply_pre_patch_hooks(ctx, op, relative_path, value)
                self._apply_delta(idx, bizobj, ctx, op, relative_path, value)
                self._apply_post_patch_hooks(ctx, op, relative_path, value)
            except JsonPatchStopSignal:
                cancel_propagation = True
            if cancel_propagation:
                break

    @staticmethod
    def _build_relative_path(ctx, idx) -> list:
        return [
            JsonPatchPathComponent(k, v) for (k, v) in
            zip(ctx['tokenized_path'][idx:],
                ctx['objs'][idx:])
            ]

    def _apply_delta(self, idx, bizobj, ctx, op, relative_path, value):
        custom_apply_delta = self.get_patch_hook(ctx['path'])
        if custom_apply_delta:
            custom_apply_delta(op, relative_path, value)
        else:
            bizobj.apply_delta(op, relative_path, value)

    def _apply_pre_patch_hooks(self, ctx, op, relative_path, value):
        for k in self.get_pre_patch_hooks(ctx['path']):
            getattr(self, k)(op, relative_path, value)

    def _apply_post_patch_hooks(self, ctx, op, relative_path, value):
        for k in self.get_post_patch_hooks(ctx['path']):
            getattr(self, k)(op, relative_path, value)

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

        def die(fstr, *args):
            raise JsonPatchError(fstr.format(*args))

        obj = path[-2].obj
        sub_obj = path[-1].obj
        key = path[-1].key_in_parent

        if op == OP_DELTA_REMOVE:
            if key.isdigit():
                if not isinstance(obj, list):
                    die('{} not a valid list', obj)
                idx = int(key)
                new_obj = obj[:idx] + obj[idx+1:]
                obj.clear()
                obj.extend(new_obj)
            elif isinstance(obj, dict):
                if key not in obj:
                    die('key "{}" not found in {}', key, obj)
                obj[key] = None
            elif is_bizobj(obj):
                attr = getattr(obj, key, None)
                if attr is None:
                    die('{} has no attribute "{}"', obj, key)
                setattr(obj, key, None)
            return

        is_list = isinstance(obj, list)
        is_list_subobj = isinstance(sub_obj, list)

        if op == OP_DELTA_ADD:
            if is_list_subobj:
                sub_obj.append(value)
            elif isinstance(obj, dict):
                if key in obj:
                    die('key "{}" already in {}', key, obj)
                obj[key] = value
            elif is_bizobj(obj):
                attr = getattr(obj, key, None)
                if attr is not None:
                    die('{} already has attribute "{}"', obj, key)
                setattr(obj, key, value)
        elif op == OP_DELTA_REPLACE:
            if is_list:
                idx_str = key
                if not (idx_str and idx_str.isdigit()):
                    die('no list index specified')
                idx = int(idx_str)
                if not (0 <= idx < len(obj)):
                    die('list index out of bounds')
                obj[idx] = value
            elif isinstance(obj, dict):
                if key not in obj:
                    die('key "{}" not found in {}', key, obj)
                obj[key] = value
            elif is_bizobj(obj):
                if not hasattr(obj, key):
                    die('{} has no attribute "{}"', obj, key)
                setattr(obj, key, value)

    def _build_patch_context(self, op, path: str, value=None):
        tokenized_path = self._parse_path(path)
        obj = self
        objs = [obj]
        bizobjs = [(0, obj)]

        for i, token in enumerate(tokenized_path):
            if token == ROOT_ATTR:
                continue
            if obj is None:
                break
            is_bizobj = is_bizobj(obj)
            if is_bizobj:
                sub_obj = getattr(obj, token, None)
            elif token.isdigit():
                sub_obj = obj[int(token)]
            else:
                assert isinstance(obj, dict)
                if token in obj:
                    sub_obj = obj[token]
                else:
                    # this likely means we are
                    # add ing a new key to a dict
                    sub_obj = None

            objs.append(sub_obj)

            if is_bizobj(sub_obj):
                bizobjs.append((i, sub_obj))

            obj = sub_obj

        return {
            'path': path,
            'tokenized_path': tokenized_path,
            'bizobjs': bizobjs,
            'objs': objs,
            }

    def _parse_path(self, path: str):
        path_crumbs = [ROOT_ATTR]
        path_crumbs.extend(x for x in path.strip('/').split('/') if x)
        if len(path_crumbs) == 1:
            raise JsonPatchError('invalid JsonPatch path')
        return path_crumbs


class JsonPatchPathComponent(object):
    def __init__(self, key_in_parent, obj):
        self._item = (key_in_parent, obj)

    def __getitem__(self, idx):
        return self._item[idx]

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


class JsonPatchError(Exception):
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
