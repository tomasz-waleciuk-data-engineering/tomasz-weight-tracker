from pathlib import Path


def nth_level_parent_dir(base_path: str = None, levels: int = 0):
    '''
    Generate a parent dir up to the given level.

    Parameters
    ----------
    base_path (str): optional
        If not provided than __file__ will be used.
    levels (int): optional
        If True, add an exclamation mark.

    Returns
    -------
    pathlib 'Path'
        a parent dir up to the given level
    '''
    # resolve the dir containing the current *.py module
    current_file_directory = Path(__file__).resolve().parent
    # if base_path provided, then use base path
    required_path = base_path or current_file_directory
    # move down the path by the required number of levels
    while levels > 0:
        required_path = Path(required_path).resolve().parent
        levels -= 1
    # return found path
    return required_path


if __name__ == '__main__':
    resolved_path = nth_level_parent_dir()
    print(resolved_path, type(resolved_path))
