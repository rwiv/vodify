from pyutils import path_join

from .segment_accessor_local import LocalSegmentAccessor
from .segment_accessor_s3 import S3SegmentAccessor
from .segment_accessor import SegmentAccessor
from ..schema.recnode_constrants import RECNODE_INCOMPLETE_DIR_NAME
from ...common.fs import FsType, FsConfig
from ...env import WorkerEnv
from ...external.s3 import S3AsyncClient


def create_recnode_accessor(
    fs_name: str,
    fs_configs: list[FsConfig],
    env: WorkerEnv,
) -> SegmentAccessor:
    fs_conf: FsConfig | None = None
    for conf in fs_configs:
        if conf.name == fs_name:
            fs_conf = conf
            break
    if fs_conf is None:
        raise ValueError(f"fs_name not found: {fs_name}")
    if fs_conf.type == FsType.LOCAL:
        return LocalSegmentAccessor(
            local_incomplete_dir_path=path_join(env.recnode.base_dir_path, RECNODE_INCOMPLETE_DIR_NAME),
        )
    elif fs_conf.type == FsType.S3:
        if fs_conf.s3 is None:
            raise ValueError(f"fs_conf.s3 is None")
        s3_client = S3AsyncClient(
            conf=fs_conf.s3,
            network_mbit=env.network_mbit,
            network_buf_size=env.network_buf_size,
            retry_limit=env.network_retry_limit,
            min_read_timeout_sec=env.min_read_timeout_sec,
            read_timeout_threshold=env.read_timeout_threshold,
            proxy_conf=env.proxy,
        )
        return S3SegmentAccessor(s3_client=s3_client, delete_batch_size=env.recnode.delete_batch_size)
    else:
        raise ValueError(f"Unknown fs_name: {fs_name}")
