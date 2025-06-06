import os
import shutil
from pathlib import Path

import yaml
from pyutils import path_join, get_ext, log, filename

from .loss_config import read_loss_config, LossConfig, LossCommand, LossMethod
from .loss_inspector import LossInspector, InspectResult
from .loss_inspector_size import SizeLossInspector
from .loss_inspector_time import TimeLossInspector
from ..env import BatchEnv
from ..notifier import create_notifier
from ...utils import check_dir, read_dir_recur


class LossExecutor:
    def __init__(self, env: BatchEnv):
        conf_path = env.loss_config_path
        if conf_path is None:
            raise ValueError("loss_config_path is required")
        self.conf = read_loss_config(conf_path)
        self.inspector = create_inspector(self.conf)
        self.src_dir_path = self.conf.src_dir_path
        self.out_dir_path = self.conf.out_dir_path
        self.tmp_dir_path = self.conf.tmp_dir_path
        self.notifier = create_notifier(env=env.env, conf=env.untf)

    def run(self):
        if self.conf.method == LossMethod.INSPECT:
            self.__inspect()
        elif self.conf.method == LossMethod.ANALYZE:
            self.__analyze()
        else:
            raise ValueError(f"Unknown method: {self.conf.method}")

    def __inspect(self):
        if not Path(self.tmp_dir_path).exists():
            os.makedirs(self.tmp_dir_path, exist_ok=True)

        for file_path in read_dir_recur(self.src_dir_path):
            src_sub_path = file_path.replace(self.src_dir_path, "")

            tmp_file_path = path_join(self.tmp_dir_path, filename(file_path))
            shutil.copy2(file_path, tmp_file_path)

            try:
                ext = get_ext(src_sub_path)
                tmp_csv_path = path_join(self.tmp_dir_path, src_sub_path.replace(f".{ext}", ".csv"))
                yaml_path = path_join(self.out_dir_path, src_sub_path.replace(f".{ext}", ".yaml"))

                result = self.inspector.inspect(tmp_file_path, tmp_csv_path)

                os.remove(tmp_file_path)
                if self.conf.archive_csv:
                    out_csv_path = path_join(self.out_dir_path, src_sub_path.replace(f".{ext}", ".csv"))
                    shutil.move(tmp_csv_path, out_csv_path)
                else:
                    os.remove(tmp_csv_path)

                check_dir(yaml_path)
                with open(yaml_path, "w") as file:
                    file.write(yaml.dump(result.to_out_dict(), allow_unicode=True))

                log.info("one loss check is done", get_done_log_attrs(result, file_path))
            except Exception as e:
                notify_msg = f"Directory Failed: {path_join(self.src_dir_path, src_sub_path)}, err: {e}"
                self.notifier.notify(notify_msg)
                raise

        self.notifier.notify(f"directory frame-loss check done: {self.src_dir_path}")
        log.info(f"directory frame-loss check done: {self.src_dir_path}")

    def __analyze(self):
        if not Path(self.tmp_dir_path).exists():
            os.makedirs(self.tmp_dir_path, exist_ok=True)

        for file_path in read_dir_recur(self.src_dir_path):
            src_sub_path = file_path.replace(self.src_dir_path, "")
            ext = get_ext(src_sub_path)
            if ext != "csv":
                continue

            result = self.inspector.analyze(file_path)

            yaml_path = path_join(self.out_dir_path, src_sub_path.replace(f".{ext}", ".yaml"))
            check_dir(yaml_path)
            with open(yaml_path, "w") as file:
                file.write(yaml.dump(result.to_out_dict(), allow_unicode=True))

            log.info("one loss check is done", get_done_log_attrs(result, file_path))

        log.info(f"directory frame-loss check done", {"dir_path": self.src_dir_path})


def get_done_log_attrs(inspect_result: InspectResult, file_path: str) -> dict:
    return {"file_path": file_path, "elapsed_time": inspect_result.elapsed_time}


def create_inspector(conf: LossConfig) -> LossInspector:
    if conf.command == LossCommand.TIME_KEY:
        return TimeLossInspector(keyframe_only=True)
    elif conf.command == LossCommand.TIME_ALL:
        return TimeLossInspector(keyframe_only=False)
    elif conf.command == LossCommand.SIZE:
        return SizeLossInspector(conf.size)
    else:
        raise ValueError(f"Unknown command: {conf.command}")
