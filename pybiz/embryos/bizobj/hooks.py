def pre_create(context):
    context['biz_class_name'] = context['args']['name']
    context['dao_class_name'] = context['args']['name'] + 'Dao'

    context['biz_dir'] = context['args'].get('biz-dir', 'biz')
    context['dao_dir'] = context['args'].get('dao-dir', 'dao')
    context['api_dir'] = context['args'].get('api-dir', 'api')
