def pre_create(context):
    args = context['args']
    name = args.get('name', context.get('name'))
    description = args.get('description', context.get('description'))
    version = args.get('version', context.get('version'))
    tagline = args.get('tagline', context.get('tagline'))

    context['name'] = name
    context['description'] = description
    context['version'] = version
    context['tagline'] = tagline
