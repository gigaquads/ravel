from embryo import hooks

from pybiz.embryos.python_project.schema import PythonProjectSchema


def pre_create(context):
    context = hooks.PreCreateHook.from_schema(PythonProjectSchema, context)
    return context
