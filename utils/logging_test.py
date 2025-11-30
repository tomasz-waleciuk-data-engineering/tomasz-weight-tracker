import logging
import sys
import argparse

from file_system_utils import nth_level_parent_dir

logger = logging.getLogger(__name__)


def _ensure_log_directory(base_path=None):
    """Ensure the logs directory exists.
    You can provide a custom path 'base_path'
    or keep 'logs' subdir in the project root folder"""
    
    # resolves project root, provided this module is contained
    # in a subdirectory, hence '1 level down'
    project_root = nth_level_parent_dir(base_path, 1)
    
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


logger = logging.getLogger(__name__)
_ensure_log_directory()
print(__name__)
print(__file__)

print(sys.argv)

project_root = nth_level_parent_dir(levels=1)

print(project_root)


parser = argparse.ArgumentParser(
    description='This will examine command line arguments and pass the correct ones'
    )

parser.add_argument(
    '--log',
    type=str,
    default='DEBUG',
    # Optional: Restrict allowed values
    choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
    help='Set the logging level (e.g., INFO, DEBUG)',
    )

parser.add_argument(
    '--arg',
    type=str,
    default=None,
    # # Optional: Restrict allowed values
    # choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
    help='New argument called "arg"',
    )

all_args = parser.parse_args()
# Access the log level directly from args object
log_level = all_args.log
print(f'Extra argument = {all_args.arg}')

numeric_level = getattr(logging, log_level, None)
print(log_level, numeric_level)