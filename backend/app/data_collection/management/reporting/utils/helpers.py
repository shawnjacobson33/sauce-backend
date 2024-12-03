import os
from collections import deque


def convert_deque_and_keys(data):
    """Recursively convert all deques to lists and keys in dictionaries to strings."""
    if isinstance(data, deque):
        return list(data)
    elif isinstance(data, list):
        return [convert_deque_and_keys(element) for element in data]
    elif isinstance(data, dict):
        return {str(key): convert_deque_and_keys(value) for key, value in data.items()}
    return data


def get_file_path(report_type: str, is_secondary: bool = False, secondary_type: str = None) -> str:
    # form the file path
    file_path = f'reports/primary/{report_type}.json' if not is_secondary else f'reports/secondary/{secondary_type}/{report_type}.json'
    # make any directories that don't already exist
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    # return file path
    return file_path