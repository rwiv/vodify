from ..common.env import get_worker_env, get_celery_env
from ..common.fs import read_fs_config
from ..common.notifier import create_notifier
from ..common.status import TaskStatusRepository
from ..service.stdl.transcoder import StdlTranscoder, create_stdl_helper


class WorkerDependencyManager:
    def __init__(self):
        self.celery_env = get_celery_env()
        self.worker_env = get_worker_env()
        self.task_status_repository = TaskStatusRepository(self.celery_env.redis)

    def create_stdl_transcoder(self, src_fs_name: str) -> StdlTranscoder:
        env = self.worker_env
        fs_configs = read_fs_config(env.fs_config_path)
        notifier = create_notifier(env=env.env, conf=env.untf)
        return StdlTranscoder(
            accessor=create_stdl_helper(fs_name=src_fs_name, fs_configs=fs_configs, env=env),
            notifier=notifier,
            out_dir_path=env.stdl.base_dir_path,
            tmp_path=env.tmp_dir_path,
            is_archive=env.stdl.is_archive,
            video_size_limit_gb=env.stdl.video_size_limit_gb,
        )

    def read_env(self):
        return get_worker_env()
