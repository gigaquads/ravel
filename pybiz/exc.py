class PyBizError(Exception):
    pass


class NotFound(Exception):
    pass


class ApiError(PyBizError):
    pass


class ManifestError(PyBizError):
    pass
