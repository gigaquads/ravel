class PybizError(Exception):
    def __init__(self, message, data=None, traceback_depth=None):
        super().__init__(message)
        self.data = data or {}
        self.traceback_depth = traceback_depth

    def to_dict(self):
        return {
            'message': str(self),
            'data': self.data,
        }


class RelationshipError(PybizError):
    pass


class NotFound(PybizError):
    pass


class NotAuthorized(PybizError):
    pass


class ManifestError(PybizError):
    pass


class ValidationError(PybizError):
    pass


class RelationshipArgumentError(PybizError):
    pass


class BizObjectError(PybizError):
    pass
