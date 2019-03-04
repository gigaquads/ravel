class PybizError(Exception):
    pass


class RelationshipError(PybizError):
    pass


class NotFound(Exception):
    pass


class ApiError(PybizError):
    pass


class ManifestError(PybizError):
    pass
