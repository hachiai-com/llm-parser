"""
logger.py
---------
Mirrors the Java Log4j logging setup from DynamicTemplateLLMParser / LLMParser.

Java pattern  : %d{yyyy-MM-dd HH:mm:ss} %-5p %c{1}:%L - %m%n
Python equiv  : %(asctime)s %(levelname)-5s %(name)s:%(lineno)d - %(message)s

Behaviour mirrored from Java:
  - Every parser run gets its own FileAppender  keyed to a unique execution_id
  - A StreamHandler (console / stdout) is always attached  → "Both"
  - additivity = False  →  logger.propagate = False (no double-logging to root)
  - Static / class-level logger still available via get_static_logger()
"""

import logging
import os
import sys
from typing import Optional


# ---------------------------------------------------------------------------
# Format exactly matches the Java PatternLayout
# ---------------------------------------------------------------------------
LOG_FORMAT = "%(asctime)s %(levelname)-5s %(name)s:%(lineno)d - %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# Directory where per-execution log files are written  (mirrors "logs/<id>.log")
LOGS_DIR = "logs"


def _ensure_logs_dir() -> None:
    """Create the logs/ directory if it does not already exist."""
    os.makedirs(LOGS_DIR, exist_ok=True)


def get_static_logger(class_name: str) -> logging.Logger:
    """
    Returns a module/class-level logger that writes only to the console.

    Mirrors:
        private static final org.apache.log4j.Logger staticLogger =
            org.apache.log4j.Logger.getLogger(DynamicTemplateLLMParser.class);

    Args:
        class_name: Typically __name__ or the class name string.

    Returns:
        A configured Logger instance.
    """
    logger = logging.getLogger(class_name)

    # Avoid adding duplicate handlers if already configured
    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)
    logger.propagate = False  # additivity = false

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT))
    logger.addHandler(console_handler)

    return logger


def get_instance_logger(execution_id: str, class_name: Optional[str] = None) -> logging.Logger:
    """
    Creates and returns a per-execution logger that writes to BOTH:
      1. logs/<execution_id>.log   (FileHandler)
      2. stdout                    (StreamHandler / console)

    Mirrors the Java configureInstanceLogger() method:
        logger = Logger.getLogger("DynamicTemplateLLMParser_" + executionId);
        FileAppender appender = new FileAppender(new PatternLayout(...), logFileName);
        logger.setAdditivity(false);

    Args:
        execution_id : UUID string that uniquely identifies this parser run.
        class_name   : Optional label used as the logger name prefix.
                       Defaults to "Parser" when not supplied.

    Returns:
        A fully configured Logger instance.
    """
    _ensure_logs_dir()

    prefix = class_name or "Parser"
    logger_name = f"{prefix}_{execution_id}"
    logger = logging.getLogger(logger_name)

    # Guard: if handlers already added (e.g. called twice), return as-is
    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)
    logger.propagate = False  # additivity = false — no root logger bleed-through

    formatter = logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT)

    # ── 1. File handler ────────────────────────────────────────────────────
    log_file_path = os.path.join(LOGS_DIR, f"{execution_id}.log")
    file_handler = logging.FileHandler(log_file_path, mode="a", encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # ── 2. Console / stdout handler ────────────────────────────────────────
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger


def close_instance_logger(logger: logging.Logger) -> None:
    """
    Flushes and closes all handlers attached to the given logger.

    Mirrors the Java finally block in executeMatchedParser():
        Enumeration appenders = logger.getAllAppenders();
        while (appenders.hasMoreElements()) {
            Appender appender = (Appender) appenders.nextElement();
            logger.removeAppender(appender);
            appender.close();
        }
    """
    handlers = logger.handlers[:]      # copy — we mutate during iteration
    for handler in handlers:
        try:
            handler.flush()
            handler.close()
        except Exception:
            pass
        logger.removeHandler(handler)