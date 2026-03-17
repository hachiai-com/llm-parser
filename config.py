"""
config.py
---------
Centralized configuration loader for the LLM Parser toolkit.

Replaces Java's:
    Parser.readAppProperties()  →  reads application.properties
    prop.getProperty(key, default)

Now reads from a .env file using python-dotenv.

Usage:
    from config import cfg

    db_url   = cfg("DATABASE_URL")
    timeout  = cfg("HACHIAI_LLM_HTTP_TIMEOUT", "180")

Install dependency:
    pip install python-dotenv
"""

import os
from typing import Optional
from dotenv import load_dotenv, find_dotenv


# ---------------------------------------------------------------------------
# Load .env on import
# ---------------------------------------------------------------------------
# find_dotenv() walks up from CWD until it finds a .env file —
# mirrors Java's classpath / root-path fallback in readAppProperties()
# _env_path = find_dotenv(usecwd=True)
# if _env_path:
#     load_dotenv(_env_path, override=False)
# else:
#     # Fallback: try script directory
#     _script_dir = os.path.dirname(os.path.abspath(__file__))
#     _fallback   = os.path.join(_script_dir, ".env")
#     if os.path.isfile(_fallback):
#         load_dotenv(_fallback, override=False)


def cfg(key: str, default: Optional[str] = None) -> Optional[str]:
    """
    Read a config value from the environment (loaded from .env).

    Mirrors: prop.getProperty(key, defaultValue)

    Args:
        key     : Environment variable name (e.g. "DATABASE_URL").
        default : Returned when the key is not set.

    Returns:
        String value or default.
    """
    return os.environ.get(key, default)


def cfg_int(key: str, default: int = 0) -> int:
    """Convenience — returns int, mirrors Integer.parseInt(prop.getProperty(...))"""
    val = cfg(key)
    try:
        return int(val) if val is not None else default
    except ValueError:
        return default


def cfg_bool(key: str, default: bool = False) -> bool:
    """Convenience — returns bool, mirrors Boolean.parseBoolean(prop.getProperty(...))"""
    val = cfg(key)
    if val is None:
        return default
    return val.strip().lower() == "true"


def cfg_float(key: str, default: float = 0.0) -> float:
    """Convenience — returns float."""
    val = cfg(key)
    try:
        return float(val) if val is not None else default
    except ValueError:
        return default


# ---------------------------------------------------------------------------
# Typed accessors grouped by concern
# Mirrors the prop.getProperty() calls scattered across the Java classes
# ---------------------------------------------------------------------------

class DBConfig:
    """Mirrors the database.* properties used in SqlDao constructor calls."""
    type = lambda: cfg("DATABASE_TYPE", "mysql")
    host = lambda: cfg("DATABASE_HOST", "localhost")
    port = lambda: cfg_int("DATABASE_PORT", 3306)
    username = lambda: cfg("DATABASE_USERNAME", "")
    password = lambda: cfg("DATABASE_PASSWORD", "")
    database_name = lambda: cfg("DATABASE_NAME", "")


class LLMConfig:
    """Mirrors hachiai.llm.* properties used across LLMParser and DynamicTemplateLLMParser."""

    # Auth
    token               = lambda: cfg("HACHIAI_LLM_TOKEN", "")

    # Model settings
    llm_type            = lambda: cfg("HACHIAI_LLM_TYPE", "llm")
    max_token           = lambda: cfg("HACHIAI_LLM_MAX_TOKEN", "8096")
    temperature         = lambda: cfg("HACHIAI_LLM_TEMPERATURE", "0.00")
    http_timeout        = lambda: cfg_int("HACHIAI_LLM_HTTP_TIMEOUT", 180)
    enable_validation   = lambda: cfg("HACHIAI_LLM_ENABLE_VALIDATION", "false")
    enable_confidence   = lambda: cfg("HACHIAI_LLM_ENABLE_CONFIDENCE_SCORE", "false")

    # Endpoints
    api_url             = lambda: cfg("HACHIAI_LLM_API", "")
    status_api_url      = lambda: cfg("HACHIAI_LLM_STATUS_API_URL", "")
    conversation_api    = lambda: cfg("HACHIAI_LLM_CONVERSATION_API", "")

    # QnA status polling
    status_total_iterations   = lambda: cfg_int("HACHIAI_LLM_STATUS_API_TOTAL_ITERATIONS", 3)
    status_interval_minutes   = lambda: cfg_int("HACHIAI_LLM_STATUS_API_INTERVAL_MINUTES", 3)

    # Conversation API polling
    conv_total_iterations     = lambda: cfg_int("HACHIAI_LLM_CONVERSATION_API_TOTAL_ITERATIONS", 1)
    conv_interval_seconds     = lambda: cfg_int("HACHIAI_LLM_CONVERSATION_API_INTERVAL_SECONDS", 10)

    # Dynamic parser query prompt
    dynamic_parser_query      = lambda: cfg(
        "HACHIAI_LLM_DYNAMIC_PARSER_QUERY",
        "Give me title of this document along with the company/vendor name?"
    )



# def load_env(env_file_path: str) -> None:

#     """
#     Load a specific .env file, overriding any already-loaded values.
#     Called from handle_request when a client provides their own env_file path.
    
#     Args:
#         env_file_path: Absolute or relative path to the client's .env file.
#     """
#     if not os.path.isfile(env_file_path):
#         raise FileNotFoundError(f".env file not found: '{env_file_path}'")
#     load_dotenv(env_file_path, override=True)