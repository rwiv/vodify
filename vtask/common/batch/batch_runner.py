import asyncio
import logging

from pyutils import log

from ..env import get_batch_env, BatchCommand
from ..loss import LossExecutor
from ...encoding import EncodingExecutor
from ...stdl import StdlArchiveExecutor
from ...video import VideoDownloadExecutor


class BatchRunner:
    def run(self):
        log.set_level(logging.DEBUG)
        env = get_batch_env()
        if env.command == BatchCommand.LOSS:
            return LossExecutor(env).run()
        if env.command == BatchCommand.VIDEO:
            return asyncio.run(VideoDownloadExecutor(env).run())
        if env.command == BatchCommand.ARCHIVE:
            return asyncio.run(StdlArchiveExecutor(env).run())
        if env.command == BatchCommand.ENCODING:
            return asyncio.run(EncodingExecutor(env).run())
        else:
            raise ValueError(f"Unknown command: {env.command}")
