from ..common.env import get_worker_env
from ..common.fs import read_fs_config
from ..service.stdl import StdlMuxer, create_stdl_helper


class WorkerDependencyManager:

    def create_stdl_muxer(self, src_fs_name: str) -> StdlMuxer:
        env = self.read_env()
        fs_configs = read_fs_config(env.fs_config_path)
        return StdlMuxer(
            helper=create_stdl_helper(fs_name=src_fs_name, fs_configs=fs_configs, env=env),
            base_path=env.stdl.base_dir_path,
            tmp_path=env.tmp_dir_path,
            is_archive=env.stdl.is_archive,
        )

    def read_env(self):
        return get_worker_env()


deps = WorkerDependencyManager()
