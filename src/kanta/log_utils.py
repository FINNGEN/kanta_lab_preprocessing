import logging
from datetime import timedelta
from pathlib import Path


def configure_logging(log_file: Path, *, level: int = logging.INFO) -> None:
    """Attach a console + file handler to the "kanta" logger, shared by all kanta.* loggers."""
    logger = logging.getLogger("kanta")
    logger.setLevel(level)

    formatter = logging.Formatter("%(asctime)s - %(levelname)-8s - %(message)s", "%Y-%m-%d %H:%M:%S")

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    file_handler = logging.FileHandler(log_file, mode="w")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)


def format_duration(seconds: float) -> str:
    """Format a duration in seconds as H:MM:SS, for logging step timings."""
    return str(timedelta(seconds=round(seconds)))
