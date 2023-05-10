# Standard Python library imports
import os
from datetime import datetime


class Utility:

    def __init__(self):
        self.today = datetime.today().date()

    @staticmethod
    def empty_the_folder(folder_path: str):

        # Get all the file names in the folder
        file_names = os.listdir(folder_path)

        # Loop through the file names and delete them
        for file_name in file_names:
            file_path = os.path.join(folder_path, file_name)
            os.remove(file_path)