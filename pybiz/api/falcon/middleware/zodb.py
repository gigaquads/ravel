from pybiz.dao.zodb_dao import ZodbDao


class ZodbDaoMiddleware(object):
    def __init__(self, db: str):
        super().__init__()
        ZodbDao.connect(db)

    def process_request(self, request, response):
        pass

    def process_resource(self, request, response, resource, params):
        pass

    def process_response(self, request, response, resource):
        status_code = int(response.status[:3])
        if 200 <= status_code < 300:
            try:
                ZodbDao.commit()
            except Exception:
                ZodbDao.rollback()
        else:
            ZodbDao.rollback()
