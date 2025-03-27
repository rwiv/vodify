from pyutils import path_join

from .stdl_helper_local import StdlLocalHelper
from .stdl_helper_s3 import StdlS3Helper
from .stdl_constrants import STDL_INCOMPLETE_DIR_NAME
from .stdl_helper import StdlHelper
from ...common.env import WorkerEnv
from ...common.fs import FsType, FsConfig
from ...utils import disable_warning_log


def create_stdl_helper(
    fs_name: str,
    fs_configs: list[FsConfig],
    env: WorkerEnv,
) -> StdlHelper:
    fs_conf: FsConfig | None = None
    for conf in fs_configs:
        if conf.name == fs_name:
            fs_conf = conf
            break
    if fs_conf is None:
        raise ValueError(f"fs_name not found: {fs_name}")
    incomplete_dir_path = path_join(env.stdl.base_dir_path, STDL_INCOMPLETE_DIR_NAME)
    if fs_conf.type == FsType.LOCAL:
        return StdlLocalHelper(incomplete_dir_path=incomplete_dir_path)
    elif fs_conf.type == FsType.S3:
        if fs_conf.s3 is None:
            raise ValueError(f"fs_conf.s3 is None")
        if not fs_conf.s3.verify:
            disable_warning_log()
        return StdlS3Helper(
            local_incomplete_dir_path=incomplete_dir_path,
            conf=fs_conf.s3,
            network_io_delay_ms=env.network_io_delay_ms,
            network_buf_size=env.network_buf_size,
        )
    else:
        raise ValueError(f"Unknown fs_name: {fs_name}")
