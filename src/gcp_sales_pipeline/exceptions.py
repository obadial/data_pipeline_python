from __future__ import annotations


class DataLoadError(RuntimeError):
    """Raised when data loading from an external system fails."""


class DataQualityError(RuntimeError):
    """Raised when loaded data does not match the expected schema or quality."""


class TooManyFilesError(DataLoadError):
    """Raised when too many files are detected in a GCS prefix."""
