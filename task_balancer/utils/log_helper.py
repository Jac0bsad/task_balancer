import logging
import os


def _str_to_level(level_str: str) -> int:
    mapping = {
        "CRITICAL": logging.CRITICAL,
        "ERROR": logging.ERROR,
        "WARNING": logging.WARNING,
        "INFO": logging.INFO,
        "DEBUG": logging.DEBUG,
        "NOTSET": logging.NOTSET,
    }
    return mapping.get(level_str.upper(), logging.INFO)


def _configure_logger() -> logging.Logger:
    # Use a single project-wide logger name
    log = logging.getLogger("task_balancer")

    # Respect env var for log level; default INFO
    level_str = os.getenv("TASK_BALANCER_LOG_LEVEL", "INFO")
    level = _str_to_level(level_str)
    log.setLevel(level)

    # Avoid propagating to root to prevent duplicate logs in host apps
    log.propagate = False

    if not log.handlers:
        # Console handler
        ch = logging.StreamHandler()
        ch.setLevel(level)

        fmt = os.getenv(
            "TASK_BALANCER_LOG_FORMAT",
            "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        )
        datefmt = os.getenv("TASK_BALANCER_LOG_DATEFMT", "%Y-%m-%d %H:%M:%S")
        ch.setFormatter(logging.Formatter(fmt=fmt, datefmt=datefmt))
        log.addHandler(ch)

        # Optional file handler
        log_file = os.getenv("TASK_BALANCER_LOG_FILE")
        if log_file:
            fh = logging.FileHandler(
                log_file, mode=os.getenv("TASK_BALANCER_LOG_FILE_MODE", "a")
            )
            fh.setLevel(level)
            fh.setFormatter(logging.Formatter(fmt=fmt, datefmt=datefmt))
            log.addHandler(fh)

    return log


# The single shared logger to be imported across the project
logger: logging.Logger = _configure_logger()


def get_logger() -> logging.Logger:
    """Return the project-wide logger.

    Prefer importing `logger` directly, but this helper is available
    for cases where a function needs to obtain it lazily.
    """
    return logger


def set_level(level: int | str) -> None:
    """Dynamically update logger level at runtime.

    Accepts `logging` levels or their string names.
    """
    if isinstance(level, str):
        level = _str_to_level(level)
    logger.setLevel(level)
    for h in logger.handlers:
        h.setLevel(level)
