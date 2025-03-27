import os

from pydantic import BaseModel, constr


DEFAULT_NETWORK_IO_DELAY_MS = 0
DEFAULT_BUFFER_SIZE = 65536


class WorkerConfig(BaseModel):
    name: constr(min_length=1)


class StdlConfig(BaseModel):
    base_dir_path: constr(min_length=1)
    is_archive: bool


class WorkerEnv(BaseModel):
    env: constr(min_length=1)
    tmp_dir_path: constr(min_length=1)
    out_fs_name: constr(min_length=1)
    fs_config_path: constr(min_length=1)
    network_io_delay_ms: int
    network_buf_size: int
    worker: WorkerConfig
    stdl: StdlConfig


def get_worker_env() -> WorkerEnv:
    env = os.getenv("PY_ENV")
    if env is None:
        env = "dev"

    worker_config = WorkerConfig(name=os.getenv("WORKER_NAME"))
    stdl_config = StdlConfig(
        base_dir_path=os.getenv("STDL_BASE_DIR_PATH"),
        is_archive=os.getenv("STDL_IS_ARCHIVE") == "true",
    )

    return WorkerEnv(
        env=env,
        worker=worker_config,
        tmp_dir_path=os.getenv("TMP_DIR_PATH"),
        out_fs_name=os.getenv("OUT_FS_NAME"),
        fs_config_path=os.getenv("FS_CONFIG_PATH"),
        network_io_delay_ms=os.getenv("NETWORK_IO_DELAY_MS") or DEFAULT_NETWORK_IO_DELAY_MS,  # type: ignore
        network_buf_size=os.getenv("NETWORK_BUF_SIZE") or DEFAULT_BUFFER_SIZE,  # type: ignore
        stdl=stdl_config,
    )
