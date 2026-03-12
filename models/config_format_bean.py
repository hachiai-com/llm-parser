from typing import Optional, List
from pydantic import Field, BaseModel


class ConfigFormatBean(BaseModel):
    """
    Defines a database column mapping and its expected data format.
    Used as the base class for database column mapping configurations.
    """

    id: Optional[int] = Field(
        None,
        description="Unique identifier for the column mapping configuration."
    )

    sql_column_name: Optional[str] = Field(
        None,
        description="Name of the SQL column associated with the mapping."
    )

    data_type: Optional[str] = Field(
        None,
        description="Data type expected for the column."
    )

    format: Optional[List[str]] = Field(
        None,
        description="List of acceptable formats used to parse the column value."
    )