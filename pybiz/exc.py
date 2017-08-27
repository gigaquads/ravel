class PyBizError(Exception):
    pass


class ApiError(PyBizError):
    pass


class ManifestError(PyBizError):
    pass
