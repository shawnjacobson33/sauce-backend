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


def get_file_path(report_type: str, is_secondary: bool = False, is_problem: bool = False) -> str:
    # form the file path
    file_path = f'reports/primary/{report_type}.json' if not is_secondary else f'reports/secondary/{"problem" if is_problem else "relevant"}/{report_type}.json'
    # make any directories that don't already exist
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    # return file path
    return file_path