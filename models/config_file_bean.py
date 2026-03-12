from typing import Optional
from datetime import datetime
from pydantic import Field, BaseModel


class ConfigFileBean(BaseModel):
    """
    Represents metadata about a configuration file processed by the system.

    Tracks the file name, when processing started, and the processing status.
    """

    fileName: Optional[str] = Field(
        None,
        description="Name of the configuration file."
    )

    startDate: Optional[datetime] = Field(
        None,
        description="Timestamp indicating when processing of the file started."
    )

    status: Optional[str] = Field(
        None,
        description="Current processing status of the configuration file."
    )