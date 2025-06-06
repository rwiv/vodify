from pyutils import log, error_dict

from .video_download_config import read_video_download_config
from ..video_downloader import VideoDownloader
from ...common.env import BatchEnv
from ...common.notifier import create_notifier


class VideoDownloadExecutor:
    def __init__(self, env: BatchEnv):
        conf_path = env.video_download_config_path
        if conf_path is None:
            raise ValueError("video_download_config_path is required")
        self.conf = read_video_download_config(conf_path)
        self.downloader = VideoDownloader(
            out_dir_path=self.conf.out_dir_path,
            tmp_dir_path=self.conf.tmp_dir_path,
            ctx=self.conf.context,
        )
        self.notifier = create_notifier(env=env.env, conf=env.untf)
        self.topic = env.untf.topic

    async def run(self):
        try:
            for url in self.conf.urls:
                log.info("Start video download", {"url": url})
                await self.downloader.download(url=url, is_m3u8_url=self.conf.is_m3u8_url)
                log.info("End video download", {"url": url})
        except Exception as e:
            log.error("Video download failed", error_dict(e))
            await self.notifier.notify("Video download failed")
            raise

        log.info("All video downloads are done")
        await self.notifier.notify("All video downloads are done")
