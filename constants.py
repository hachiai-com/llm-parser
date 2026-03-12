"""
constants.py
------------
Python equivalent of com.uxplore.utils.common.Constants

Mirrors all enums and string constants used across LLMParser /
DynamicTemplateLLMParser and their dependencies.
"""

from enum import Enum

# Version
VERSION_NO = "1.5.0"

# Database type identifiers  (used by SqlDao and config loading)
MYSQL_DB = "mysql"
MSSQL_DB = "mssql"

# Column data-type sentinel  — skipped during INSERT column list building
DATA_IGNORE = "IGNORE"


# Enums
class ParserType(Enum):
    """Mirrors Constants.ParserType"""
    CSV        = "CSV"
    PDF        = "PDF"
    EXCEL      = "EXCEL"
    EMAIL_BODY = "EMAIL_BODY"


class HtmlSelectorType(Enum):
    """Mirrors Constants.HtmlSelectorType"""
    XPATH        = "XPATH"
    CSS_SELECTOR = "CSS_SELECTOR"


class DataSource(Enum):
    """
    Mirrors Constants.DataSource.
    @JsonValue / @JsonCreator → use .value for serialisation.
    """
    MODEL_FIELDS  = "ModelFields"
    KEY_VALUE_PAIRS = "KeyValuePairs"
    TEXT_DATA     = "TextData"
    COORDINATES   = "Coordinates"

    @classmethod
    def from_string(cls, value: str) -> "DataSource":
        """Mirrors the @JsonCreator fromString() method — case-insensitive."""
        for member in cls:
            if member.value.lower() == value.lower():
                return member
        raise ValueError(f"No DataSource enum constant for value: '{value}'")


class FileExecutionStatus(Enum):
    """
    Mirrors Constants.FileExecutionStatus.
    Used to track per-file processing outcomes.
    """
    Waiting    = "Waiting"
    Unapproved = "Unapproved"
    Failed     = "Failed"
    Completed  = "Completed"

    def __str__(self) -> str:
        return self.value


class TaskExecutionStatus(Enum):
    """
    Mirrors Constants.TaskExecutionStatus.
    Returned by the async LLM API status endpoint.
    """
    in_progress = "in_progress"
    failed      = "failed"
    completed   = "completed"
    fulfilled   = "fulfilled"