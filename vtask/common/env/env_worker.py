import os

from pydantic import BaseModel, constr

from .env_configs import UntfConfig, read_untf_env


class WorkerConfig(BaseModel):
    name: constr(min_length=1)
    queues: constr(min_length=1)


class StdlConfig(BaseModel):
    base_dir_path: constr(min_length=1)
    is_archive: bool
    video_size_limit_gb: int


class WorkerEnv(BaseModel):
    env: constr(min_length=1)
    tmp_dir_path: constr(min_length=1)
    out_fs_name: constr(min_length=1)
    fs_config_path: constr(min_length=1)
    network_mbit: float
    network_buf_size: int
    worker: WorkerConfig
    stdl: StdlConfig
    untf: UntfConfig


def get_worker_env() -> WorkerEnv:
    env = os.getenv("PY_ENV")
    if env is None:
        env = "dev"

    worker_config = WorkerConfig(name=os.getenv("WORKER_NAME"), queues=os.getenv("WORKER_QUEUES"))
    stdl_config = StdlConfig(
        base_dir_path=os.getenv("STDL_BASE_DIR_PATH"),
        is_archive=os.getenv("STDL_IS_ARCHIVE") == "true",
        video_size_limit_gb=os.getenv("STDL_VIDEO_SIZE_LIMIT_GB"),  # type: ignore
    )

    return WorkerEnv(
        env=env,
        worker=worker_config,
        tmp_dir_path=os.getenv("TMP_DIR_PATH"),
        out_fs_name=os.getenv("OUT_FS_NAME"),
        fs_config_path=os.getenv("FS_CONFIG_PATH"),
        network_mbit=os.getenv("NETWORK_MBIT"),  # type: ignore
        network_buf_size=os.getenv("NETWORK_BUF_SIZE"),  # type: ignore
        stdl=stdl_config,
        untf=read_untf_env(),
    )
