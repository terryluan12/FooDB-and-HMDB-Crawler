from logging import Logger
import os
import logging
logger: Logger = None


def createLogger(base_dir: str, log: str, error_log: str) -> Logger:
    
    os.makedirs(base_dir, exist_ok=True)

    logger = logging.getLogger(__name__)
    logging.basicConfig(filename=f'{base_dir}/{log}', level=logging.INFO)

    error_handler = logging.FileHandler(f"{base_dir}/{error_log}")
    error_handler.setLevel(logging.ERROR)

    logger.addHandler(error_handler)
    return logger

logger = createLogger("logs", "hmdb.log", "hmdb-error.log")
