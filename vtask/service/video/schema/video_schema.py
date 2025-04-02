from pydantic import BaseModel


class VideoDownloadRequest(BaseModel):
    cookie_str: str | None = None
    is_parallel: bool
    parallel_num: int
    non_parallel_delay_ms: int
