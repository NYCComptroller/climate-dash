import logging
from logging.handlers import TimedRotatingFileHandler
import pathlib

LOG_DIRECTORY = pathlib.Path('Logs')

INFO_DIR = LOG_DIRECTORY / 'Info'
WARN_DIR = LOG_DIRECTORY /'Warnings'
INFO_DIR.mkdir(exist_ok=True)
WARN_DIR.mkdir(exist_ok=True)

WARN_FILE = WARN_DIR / 'Warnings.log'

def setup_logging_for_pipeline(pipeline_filename: str):
    """
    Sets up logging so that ALL log messages (from any module) are handled by the same handlers:
    - Info-level and above go to the pipeline's log file
    - Warning-level and above go to the warnings log file
    - All messages go to the console
    """
    root_logger = logging.getLogger()
    if root_logger.hasHandlers():
        return logging.getLogger(pipeline_filename)

    formatter = logging.Formatter(
        '[%(asctime)s] %(levelname)s %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    pipeline_log_file = INFO_DIR/ f"{pipeline_filename}.log"
    pipeline_handler = TimedRotatingFileHandler(pipeline_log_file, when='midnight', backupCount=30)
    pipeline_handler.setFormatter(formatter)
    pipeline_handler.setLevel(logging.INFO)

    warn_handler = TimedRotatingFileHandler(WARN_FILE, when='midnight', backupCount=30)
    warn_handler.setFormatter(formatter)
    warn_handler.setLevel(logging.WARNING)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)

    # Set up handlers on the root logger so all loggers propagate here
    root_logger.setLevel(logging.DEBUG)
    root_logger.handlers = []
    root_logger.addHandler(pipeline_handler)
    root_logger.addHandler(warn_handler)
    root_logger.addHandler(console_handler)

    # Return a named logger for the pipeline, but all loggers will use the root handlers
    return logging.getLogger(pipeline_filename)

def setup_logging_for_main():
    root_logger = logging.getLogger()
    if root_logger.hasHandlers():
        return root_logger

    formatter = logging.Formatter(
        '[%(asctime)s] %(levelname)s %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    warn_handler = TimedRotatingFileHandler(WARN_FILE, when='midnight', backupCount=30)
    warn_handler.setFormatter(formatter)
    warn_handler.setLevel(logging.WARNING)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)

    root_logger.setLevel(logging.DEBUG)
    root_logger.handlers = []
    root_logger.addHandler(warn_handler)
    root_logger.addHandler(console_handler)

    return root_logger

def configure_notebook_logging():
    import logging
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] %(levelname)s %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

