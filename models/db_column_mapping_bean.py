from pydantic import Field
from typing import Optional

from models.config_format_bean import ConfigFormatBean


class DBColumnMappingBean(ConfigFormatBean):
    """
    Represents a mapping between extracted data and a database column.

    Inherits format and data type configuration from ConfigFormatBean.
    """
    pass