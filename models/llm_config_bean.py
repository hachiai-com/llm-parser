from typing import List, Optional
from pydantic import Field
from models.config_bean import ConfigBean
from models.db_column_mapping_bean import DBColumnMappingBean


class LLMConfigBean(ConfigBean):
    """
    Configuration model used by the Dynamic Template LLM Parser.

    Contains database configuration, template mapping logic,
    and file management rules used during the parsing workflow.
    """

    id: Optional[str] = Field(
        None,
        description="Unique identifier for the LLM parser configuration."
    )

    commonFields: Optional[List[DBColumnMappingBean]] = Field(
        None,
        description="List of database column mappings shared across templates."
    )

    llmPromptDatabaseTable: Optional[str] = Field(
        None,
        description="Database table storing LLM prompt templates."
    )

    fileNameColumn: Optional[str] = Field(
        None,
        description="Column name storing the file name being parsed."
    )

    parserFileExecutionIdColumn: Optional[str] = Field(
        None,
        description="Column used to store the parser execution ID."
    )

    templateReferenceDBTable: Optional[str] = Field(
        None,
        description="Database table used to resolve dynamic templates."
    )

    templateReferenceDBColumn: Optional[str] = Field(
        None,
        description="Column used to identify which template should be applied."
    )

    fileParsingStatusReferenceDBTable: Optional[str] = Field(
        None,
        description="Database table used to track parsing status."
    )

    moveCompletedFiles: Optional[str] = Field(
        None,
        description="Directory where successfully processed files are moved."
    )

    moveFailedFiles: Optional[str] = Field(
        None,
        description="Directory where failed files are moved."
    )

    moveUnapprovedFiles: Optional[str] = Field(
        None,
        description="Directory where unapproved files are moved."
    )

    rawTextTable: Optional[str] = Field(
        None,
        description="Database table used to store extracted raw text."
    )