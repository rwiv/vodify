from pyutils import path_join

from .stdl_segment_accessor_local import StdlLocalSegmentAccessor
from .stdl_segment_accessor_s3 import StdlS3SegmentAccessor
from .stdl_segment_accessor import StdlSegmentAccessor
from ...schema import STDL_INCOMPLETE_DIR_NAME
from .....common.env import WorkerEnv
from .....common.fs import FsType, FsConfig
from .....utils import disable_warning_log


def create_stdl_helper(
    fs_name: str,
    fs_configs: list[FsConfig],
    env: WorkerEnv,
) -> StdlSegmentAccessor:
    fs_conf: FsConfig | None = None
    for conf in fs_configs:
        if conf.name == fs_name:
            fs_conf = conf
            break
    if fs_conf is None:
        raise ValueError(f"fs_name not found: {fs_name}")
    if fs_conf.type == FsType.LOCAL:
        return StdlLocalSegmentAccessor(
            local_incomplete_dir_path=path_join(env.stdl.base_dir_path, STDL_INCOMPLETE_DIR_NAME),
        )
    elif fs_conf.type == FsType.S3:
        if fs_conf.s3 is None:
            raise ValueError(f"fs_conf.s3 is None")
        if not fs_conf.s3.verify:
            disable_warning_log()
        return StdlS3SegmentAccessor(
            conf=fs_conf.s3,
            network_io_delay_ms=env.network_io_delay_ms,
            network_buf_size=env.network_buf_size,
        )
    else:
        raise ValueError(f"Unknown fs_name: {fs_name}")
