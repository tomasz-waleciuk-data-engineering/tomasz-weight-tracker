import logging

from file_system_utils import nth_level_parent_dir

logger = logging.getLogger(__name__)
NUMBER_OF_LEVELS_FROM_PROJECT_ROOT = 1

def _ensure_log_directory(base_path=None):
    """Ensure the logs directory exists.
    You can provide a custom path 'base_path'
    or keep 'logs' subdir in the project root folder"""

    # resolves project root, provided this module is contained
    # in a subdirectory, hence '1 level down'
    project_root = nth_level_parent_dir(base_path, NUMBER_OF_LEVELS_FROM_PROJECT_ROOT)

    # logs stored in 'logs' subfolder
    log_directory = project_root / 'logs'

    # if the directory does not exist it will be created now
    # if it exists - no action (exists_ok)
    # if there are missing directories in the tree
    # those will be created as well (parents),
    # but in our case it won't happen as project_root
    # exists and we only add 'logs' subdir to the path
    print(f'log_directory.exists() - {log_directory.exists()}')
    log_directory.mkdir(parents=True, exist_ok=True)
    return log_directory


def _create_formatter():
    """Create a standard log formatter."""
    formatter = ' - '.join(
        '%(asctime)s',
        '%(levelname)s-3s', # 3 chars for alignment
        '%(filename)s:%(lineno)d',
        '%(name)s',
        '%(message)s',
        )
    # returns a logging.Formatter in the above format
    return logging.Formatter(formatter)

    #######################################################################
    # "%(asctime)s | "
    # "%(levelname)-8s | "  # -8s pads the level name to 8 chars for alignment
    # "%(filename)s:%(lineno)d | "
    # "%(funcName)s | "
    # "%(message)s"
    # %(filename)s: filename portion of the path (e.g., main.py).
    # %(lineno)d: source line number where the call was issued (e.g., 42).
    #######################################################################



def _create_handlers(log_directory, log_file, level):
    """Create file and console handlers."""
    file_handler = logging.FileHandler(log_directory / log_file)
    file_handler.setLevel(level)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)

    # receives the required formatter format
    formatter = _create_formatter()
    # applies the formatter to file and console handlers
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    return file_handler, console_handler


def setup_logger(name, log_file, level=logging.DEBUG, base_path=None):
    """Function to setup a logger; can be used in multiple modules."""
    log_directory = _ensure_log_directory(base_path)

    logger = logging.getLogger(name)
    logger.setLevel(level)

    if not logger.handlers:
        file_handler, console_handler = _create_handlers(
            log_directory, log_file, level
        )
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger
