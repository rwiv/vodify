import os
import shutil
from pathlib import Path

import yaml
from pyutils import path_join, get_ext, log, filename

from .loss_config import read_loss_config, LossConfig, LossCommand, LossMethod
from .loss_inspector import LossInspector
from .loss_inspector_all import AllFrameLossInspector
from .loss_inspector_key import KeyFrameLossInspector
from ...common.env import BatchEnv
from ...common.notifier import create_notifier
from ...utils import check_dir, read_dir_recur


class LossExecutor:
    def __init__(self, env: BatchEnv):
        self.conf = read_loss_config(env.loss_config_path)
        self.inspector = create_inspector(self.conf)
        self.src_dir_path = self.conf.src_dir_path
        self.out_dir_path = self.conf.out_dir_path
        self.tmp_dir_path = env.tmp_dir_path
        self.notifier = create_notifier(env)
        self.topic = env.untf.topic

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
                csv_path = path_join(self.out_dir_path, src_sub_path.replace(f".{ext}", ".csv"))
                yaml_path = path_join(self.out_dir_path, src_sub_path.replace(f".{ext}", ".yaml"))

                result = self.inspector.inspect(tmp_file_path, csv_path)
                os.remove(tmp_file_path)
                if not self.conf.archive_csv:
                    os.remove(csv_path)

                check_dir(yaml_path)
                with open(yaml_path, "w") as file:
                    file.write(yaml.dump(result.model_dump(by_alias=True), allow_unicode=True))

                log.info(f"video frame-loss check done: {file_path}")
            except Exception as e:
                notify_msg = f"Directory Failed: {path_join(self.src_dir_path, src_sub_path)}, err: {e}"
                self.notifier.notify(self.topic, notify_msg)
                raise

        self.notifier.notify(self.topic, f"directory frame-loss check done: {self.src_dir_path}")
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
                file.write(yaml.dump(result.model_dump(by_alias=True), allow_unicode=True))

            log.info(f"video frame-loss check done: {file_path}")

        log.info(f"directory frame-loss check done: {self.src_dir_path}")


def create_inspector(conf: LossConfig) -> LossInspector:
    if conf.command == LossCommand.KEY:
        return KeyFrameLossInspector(conf.key)
    elif conf.command == LossCommand.ALL:
        return AllFrameLossInspector(conf.all)
    else:
        raise ValueError(f"Unknown command: {conf.command}")
