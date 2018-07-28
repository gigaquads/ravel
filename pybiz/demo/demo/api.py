import os

from .service import DemoService


api = DemoService.factory()


@api(http_method='GET', url_path='/status')
def get_demo_service_status(echo=None, *args, **kwargs):
    return {
        'status': 'ok',
        'echo': echo,
    }
