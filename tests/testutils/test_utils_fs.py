from pyutils import path_join, find_project_root

from vidt.common.fs import read_fs_config, FsConfig


def read_test_fs_configs(is_prod: bool = False):
    if is_prod:
        conf_path = path_join(find_project_root(), "dev", "fs_conf.yaml")
    else:
        conf_path = path_join(find_project_root(), "dev", "fs_conf_test.yaml")

    return read_fs_config(conf_path)


def find_test_fs_config(fs_configs: list[FsConfig], fs_name: str) -> FsConfig:
    fs_conf: FsConfig | None = None
    for cur_conf in fs_configs:
        if cur_conf.name == fs_name:
            fs_conf = cur_conf
    if fs_conf is None:
        raise ValueError("Not found test fs config")
    return fs_conf
