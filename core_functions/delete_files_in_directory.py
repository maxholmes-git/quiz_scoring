import os
from typing import List


def delete_files_in_directory(file_list: List[str], directory: str = "", file_extension: str = ".xslx"):
    """
    Description:
        Deletes files where the name of the file is in file_list and is in directory.

    Args:
        directory:

        file_list:
        file_extension:

    Returns:

    """
    for file_path in [f"{directory}{file}{file_extension}" for file in file_list]:
        if os.path.exists(file_path):
            os.remove(file_path)
            print(f"File deleted: {file_path} .")
        else:
            print(f"File not found: {file_path}")
