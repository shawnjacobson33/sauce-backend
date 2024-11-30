import os

def nest(data: dict):
    nested_dict = dict()
    # Process each tuple key
    for keys, value in data.items():
        current_level = nested_dict
        for key in keys[:-1]:  # Traverse all except the last key
            current_level = current_level.setdefault(key, {})
        current_level[keys[-1]] = value  # Set the final key to the value

    return nested_dict


def get_file_path(entity_type: str, is_pending: bool) -> str:
    # get a customizable file path
    file_path = f'../utils/reports/{"problem" if is_pending else "relevant"}/{entity_type}.json'
    # make any directories that don't already exist
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    # return file path
    return file_path