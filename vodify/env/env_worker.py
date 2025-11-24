import os

from pydantic import BaseModel, constr

from .env_configs import read_untf_env
from ..external.notifier import UntfConfig
from ..utils import ProxyConfig


class WorkerConfig(BaseModel):
    name: constr(min_length=1)
    queues: constr(min_length=1)


def read_proxy_config() -> ProxyConfig:
    return ProxyConfig(
        host=os.getenv("PROXY_HOST") or None,
        port=os.getenv("PROXY_PORT") or None,  # type: ignore
        username=os.getenv("PROXY_USERNAME") or None,
        password=os.getenv("PROXY_PASSWORD") or None,
        rdns=os.getenv("PROXY_RDNS") == "true",
    )


class RecnodeConfig(BaseModel):
    base_dir_path: constr(min_length=1)
    is_archive: bool
    video_size_limit_gb: int
    delete_batch_size: int


class WorkerEnv(BaseModel):
    env: constr(min_length=1)
    tmp_dir_path: constr(min_length=1)
    out_fs_name: constr(min_length=1)
    fs_config_path: constr(min_length=1)
    network_mbit: float
    network_buf_size: int
    network_retry_limit: int
    min_read_timeout_sec: float
    read_timeout_threshold: float
    worker: WorkerConfig
    recnode: RecnodeConfig
    untf: UntfConfig
    proxy: ProxyConfig | None


def get_worker_env() -> WorkerEnv:
    env = os.getenv("PY_ENV")
    if env is None:
        env = "dev"

    worker_config = WorkerConfig(name=os.getenv("WORKER_NAME"), queues=os.getenv("WORKER_QUEUES"))
    recnode_config = RecnodeConfig(
        base_dir_path=os.getenv("RECNODE_BASE_DIR_PATH"),
        is_archive=os.getenv("RECNODE_IS_ARCHIVE") == "true",
        video_size_limit_gb=os.getenv("RECNODE_VIDEO_SIZE_LIMIT_GB"),  # type: ignore
        delete_batch_size=os.getenv("RECNODE_DELETE_BATCH_SIZE"),  # type: ignore
    )
    proxy_enabled = os.getenv("PROXY_ENABLED") == "true"

    return WorkerEnv(
        env=env,
        worker=worker_config,
        tmp_dir_path=os.getenv("TMP_DIR_PATH"),
        out_fs_name=os.getenv("OUT_FS_NAME"),
        fs_config_path=os.getenv("FS_CONFIG_PATH"),
        network_mbit=os.getenv("NETWORK_MBIT"),  # type: ignore
        network_buf_size=os.getenv("NETWORK_BUF_SIZE"),  # type: ignore
        network_retry_limit=os.getenv("NETWORK_RETRY_LIMIT"),  # type: ignore
        min_read_timeout_sec=os.getenv("MIN_READ_TIMEOUT_SEC"),  # type: ignore
        read_timeout_threshold=os.getenv("READ_TIMEOUT_THRESHOLD"),  # type: ignore
        recnode=recnode_config,
        untf=read_untf_env(),
        proxy=read_proxy_config() if proxy_enabled else None,
    )
