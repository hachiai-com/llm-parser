from typing import Optional, Dict, Any
from pydantic import Field, BaseModel

class ConfigBean(BaseModel):
    """
    Base configuration bean used across parsing and database operations.

    Contains SQL execution settings, database connection parameters,
    and optional custom metadata used by parser implementations.
    """

    skipRows: Optional[int] = Field(
        0,
        description="Number of rows to skip when reading input data."
    )

    customer_name: Optional[str] = Field(
        None,
        description="Customer identifier associated with the configuration."
    )

    preQuery: Optional[str] = Field(
        None,
        description="SQL query executed before the main parsing operation."
    )

    postQuery: Optional[str] = Field(
        None,
        description="SQL query executed after the main parsing operation."
    )

    sql: Optional[str] = Field(
        None,
        description="Primary SQL query used for database operations."
    )

    sqlUrl: Optional[str] = Field(
        None,
        description="Database connection URL."
    )

    userName: Optional[str] = Field(
        None,
        description="Database username."
    )

    password: Optional[str] = Field(
        None,
        description="Database password."
    )

    database: Optional[str] = Field(
        None,
        description="Database name used by the parser."
    )

    table: Optional[str] = Field(
        None,
        description="Target database table for data operations."
    )

    show_query: Optional[bool] = Field(
        False,
        description="Flag indicating whether SQL queries should be logged."
    )

    dry_run: Optional[bool] = Field(
        False,
        description="If True, SQL operations will not be executed."
    )

    customData: Optional[Dict[str, Any]] = Field(
        default_factory=dict,
        description="Custom key-value metadata used by parser implementations."
    )

    def add_custom_data(self, key: str, value: Any):
        """Add custom metadata to the configuration."""
        self.customData[key] = value

    def get_custom_data(self, key: str) -> Any:
        """Retrieve custom metadata value."""
        return self.customData.get(key)