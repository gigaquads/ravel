def pre_create(context):
    biz_class_name = context.get('name', context['args'].get('name'))

    context['biz_class_name'] = biz_class_name
    context['dao_class_name'] = biz_class_name + 'Dao'

    context['biz_dir'] = context['args'].get('biz-dir', 'biz')
    context['dao_dir'] = context['args'].get('dao-dir', 'dao')
    context['api_dir'] = context['args'].get('api-dir', 'api')

    context.setdefault('fields', [])
    context.setdefault('biz', [])
    context.setdefault('api', [])
    context.setdefault('dao', [])
