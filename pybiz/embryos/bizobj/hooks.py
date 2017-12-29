def pre_create(context):
    name = context.get('name', context['args'].get('name'))
    biz_class_name = name

    context['name'] = name
    context['biz_class_name'] = biz_class_name
    context['dao_class_name'] = biz_class_name + 'Dao'

    context['biz_dir'] = context['args'].get('biz-dir', 'biz')
    context['dao_dir'] = context['args'].get('dao-dir', 'dao')
    context['api_dir'] = context['args'].get('api-dir', 'api')

    context.setdefault('fields', [])
    context.setdefault('biz', [])
    context.setdefault('api', [])
    context.setdefault('dao', [])
