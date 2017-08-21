import falcon


class Request(falcon.Request):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.json = {}
