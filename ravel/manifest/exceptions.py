from ravel.exceptions import ManifestError


class ManifestFileNotFound(ManifestError):
    pass


class UnrecognizedManifestFileFormat(ManifestError):
    pass


class ManifestValidationError(ManifestError):
    pass


class ManifestInheritanceError(ManifestError):
    pass


class StoreClassNotFound(ManifestError):
    pass


class ResourceClassNotFound(ManifestError):
    pass


class DuplicateResourceClass(ManifestError):
    pass


class DuplicateStoreClass(ManifestError):
    pass


class FilesystemScanTimeout(ManifestError):
    pass