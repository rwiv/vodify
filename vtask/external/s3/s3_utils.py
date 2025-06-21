from datetime import datetime
from datetime import datetime
from typing import Any

from aiobotocore.config import AioConfig
from aiobotocore.session import get_session
from aiohttp import ClientResponse
from pyutils import error_dict
from types_aiobotocore_s3.client import S3Client

from .s3_types import S3Config


def create_client(conf: S3Config) -> S3Client:
    client = get_session().create_client(
        "s3",
        endpoint_url=conf.endpoint_url,
        aws_access_key_id=conf.access_key,
        aws_secret_access_key=conf.secret_key,
        verify=conf.verify,
        config=AioConfig(signature_version="s3v4"),
    )
    return client  # type: ignore


def _retry_error_attr(ex: Exception, retry_cnt: int, key: str) -> dict[str, Any]:
    attr = error_dict(ex)
    attr["cnt"] = retry_cnt
    attr["key"] = key
    return attr


def _parse_get_object_res_headers(res: ClientResponse):
    content_length_str = res.headers.get("Content-Length")
    last_modified_str = res.headers.get("Last-Modified")
    if not isinstance(last_modified_str, str) or not isinstance(content_length_str, str):
        raise ValueError(f"Invalid headers")
    content_length = int(content_length_str)
    last_modified = datetime.strptime(last_modified_str, "%a, %d %b %Y %H:%M:%S GMT")
    return content_length, last_modified
