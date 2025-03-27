import yaml
from pyutils import path_join, get_ext

from .frame_loss_config import read_fl_config, FrameLossConfig
from .frame_loss_inspector import FrameLossInspector
from .frame_loss_inspector_all import AllFrameLossInspector
from .frame_loss_inspector_key import KeyFrameLossInspector
from ...common.env import BatchEnv
from ...common.notifier import create_notifier
from ...utils import check_dir, read_dir_recur


class FrameLossExecutor:
    def __init__(self, env: BatchEnv):
        self.conf = read_fl_config(env.fl_config_path)
        self.inspector = create_inspector(self.conf)
        self.src_dir_path = self.conf.src_dir_path
        self.out_dir_path = self.conf.out_dir_path
        self.notifier = create_notifier(env)
        self.topic = env.untf.topic

    def run(self):
        for file_path in read_dir_recur(self.src_dir_path):
            src_sub_path = file_path.replace(self.src_dir_path, "")
            ext = get_ext(src_sub_path)
            try:
                csv_path = path_join(self.out_dir_path, src_sub_path.replace(f".{ext}", ".csv"))
                yaml_path = path_join(self.out_dir_path, src_sub_path.replace(f".{ext}", ".yaml"))
                result = self.inspector.inspect(file_path, csv_path)

                check_dir(yaml_path)
                with open(yaml_path, "w") as file:
                    file.write(yaml.dump(result.model_dump(by_alias=True), allow_unicode=True))
            except Exception as e:
                notify_msg = f"Directory Failed: {path_join(self.src_dir_path, src_sub_path)}, err: {e}"
                self.notifier.notify(self.topic, notify_msg)
                return e

        notify_msg = f"Directory Complete: {self.src_dir_path}"
        return self.notifier.notify(self.topic, notify_msg)


def create_inspector(conf: FrameLossConfig) -> FrameLossInspector:
    if conf.command == "keyFrame":
        return KeyFrameLossInspector(conf.key_frame)
    else:
        return AllFrameLossInspector(conf.all)
