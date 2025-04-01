from pyutils import path_join, find_project_root, load_dotenv


def load_test_dotenv(file_name: str):
    load_dotenv(path_join(find_project_root(), "dev", file_name))
