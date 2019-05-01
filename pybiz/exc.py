class PybizError(Exception):
    def __init__(self, message, data=None):
        super().__init__(message)
        self.data = data or {}


class RelationshipError(PybizError):
    pass


class NotFound(PybizError):
    pass


class NotAuthorizedError(PybizError):
    pass


class ManifestError(PybizError):
    pass


class ValidationError(PybizError):
    pass


class RelationshipArgumentError(PybizError):
    pass


class BizObjectError(PybizError):
    pass
