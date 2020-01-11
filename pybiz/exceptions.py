import traceback


class PybizError(Exception):
    def __init__(
        self,
        message: str,
        data: dict = None,
        wrapped_exception: Exception = None,
        logged_traceback_depth: int = None
    ):
        super().__init__(message)
        self.data = data or {}
        self.wrapped_exception = wrapped_exception
        self.logged_traceback_depth = logged_traceback_depth
        self.wrapped_traceback = None
        if wrapped_exception:
            self.wrapped_traceback = traceback.format_exc()

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
