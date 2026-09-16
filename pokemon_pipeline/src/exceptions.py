"""
Custom exceptions used by the Pokémon ETL pipeline.

Custom exceptions represent failures that are specific to
our pipeline rather than failures raised directly by Python
or third-party libraries.
"""


class PipelineError(Exception):
    """
    Base exception for pipeline-specific failures.
    """
    pass


class ExtractionError(PipelineError):
    """
    Raised when required API extraction cannot be completed.
    """
    pass


class ValidationError(PipelineError):
    """
    Raised when processed data fails validation.
    """
    pass