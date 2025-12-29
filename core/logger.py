import logging
import sys

def get_logger(name: str = "pipeline"):
    """Returns a preconfigured logger."""
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s — %(message)s")
        handler.setFormatter(fmt)
        logger.addHandler(handler)

    return logger
