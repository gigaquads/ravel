from appyratus.validation import fields
from appyratus.util import TextTransform
from embryo import Embryo, Relationship


class ResourceEmbryo(Embryo):
    """
    # Resource Embryo

    # Relationships
    - `biz`: TODO
    - `dao`: TODO
    """

    biz = Relationship(name='pybiz-project/biz', index=0, is_nested=True)
    dao = Relationship(name='pybiz-project/dao', index=0, is_nested=True)
    api = Relationship(name='pybiz-project/api', index=0, is_nested=True)

    class context_schema(Embryo.Schema):
        """
        # Context Schema
        The respective Resource schema
        """
        biz = fields.Dict()
        dao = fields.Dict()
        api = fields.Dict()

    def pre_create(self, context, *args, **kwargs):
        context['biz'] = {'name': context['resource']['name']}
        context['dao'] = {'name': context['resource']['name']}
        context['api'] = {'name': context['resource']['name']}

    def on_create(self, context, *args, **kwargs):
        manifest = self.fs['/manifest.yml'][0]
        bindings = manifest.setdefault('bindings', [])
        biz_name = TextTransform.camel(context['biz']['name'])
        dao_name = TextTransform.camel('{}Dao'.format(context['dao']['name']))
        if biz_name not in {b['biz'] for b in bindings}:
            binding = {'biz': biz_name, 'dao': dao_name}
            bindings.append(binding)
