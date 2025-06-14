import uuid
from pathlib import Path

import yaml
from aiofiles import os as aios
from pydantic import BaseModel, constr, Field
from pyutils import log, get_ext, path_join

from .encoding_request import EncodingRequest
from .video_encoder import VideoEncoder
from ..common.env import BatchEnv
from ..common.notifier import create_notifier
from ..utils import listdir_recur, move_file, copy_file2, check_dir_async, stem


class EncodingConfig(BaseModel):
    src_dir_path: constr(min_length=1) = Field(alias="srcDirPath")
    out_dir_path: constr(min_length=1) = Field(alias="outDirPath")
    tmp_dir_path: constr(min_length=1) = Field(alias="tmpDirPath")
    request: EncodingRequest


def read_encoding_config(config_path: str) -> EncodingConfig:
    if not Path(config_path).exists():
        raise FileNotFoundError(f"File not found: {config_path}")
    with open(config_path, "r") as file:
        text = file.read()
    return EncodingConfig(**yaml.load(text, Loader=yaml.FullLoader))


class EncodingExecutor:
    def __init__(self, env: BatchEnv):
        conf_path = env.encoding_config_path
        if conf_path is None:
            raise ValueError("encoding_config_path is required")
        self.conf = read_encoding_config(conf_path)
        self.src_dir_path = self.conf.src_dir_path
        self.out_dir_path = self.conf.out_dir_path
        self.tmp_dir_path = self.conf.tmp_dir_path
        self.notifier = create_notifier(env=env.env, conf=env.untf)
        self.encoder = VideoEncoder()

    async def run(self):
        if not await aios.path.exists(self.tmp_dir_path):
            await aios.makedirs(self.tmp_dir_path, exist_ok=True)

        for file_path in await listdir_recur(self.src_dir_path):
            sub_path = file_path.replace(self.src_dir_path, "")
            file_stem = f"{stem(file_path)}_{str(uuid.uuid4())}"
            ext = get_ext(file_path)
            if ext is None:
                raise ValueError(f"File without extension: {file_path}")
            tmp_src_path = path_join(self.tmp_dir_path, f"{file_stem}_src.{ext}")
            await copy_file2(file_path, tmp_src_path)

            try:
                tmp_out_path = path_join(self.tmp_dir_path, f"{file_stem}_out.{ext}")

                request: EncodingRequest = self.conf.request.copy()
                request.src_file_path = tmp_src_path
                request.out_file_path = tmp_out_path
                await self.encoder.encode(request, logging=False)
                await aios.remove(tmp_src_path)

                out_file_path = path_join(self.out_dir_path, sub_path)
                await check_dir_async(out_file_path)
                await move_file(tmp_out_path, out_file_path)
            except Exception as e:
                await aios.remove(tmp_src_path)
                await self.notifier.notify(f"Failed to encoding: {sub_path}, err={e}")
                raise

        await self.notifier.notify(f"Encoding Completed: {self.src_dir_path}")
        log.info(f"Encoding Completed: {self.src_dir_path}")
