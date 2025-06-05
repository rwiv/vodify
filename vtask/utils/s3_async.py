import asyncio
from datetime import datetime
from typing import Any

import aiofiles
import aiohttp
from aiobotocore.config import AioConfig
from aiobotocore.session import get_session
from aiofiles import os as aios
from aiohttp import ClientResponse
from botocore.exceptions import ClientError
from pydantic import BaseModel
from pyutils import log, error_dict
from types_aiobotocore_s3.client import S3Client

from .file import utime
from .limiter import nio_limiter
from .s3_responses import S3ListResponse, S3ObjectInfoResponse
from ..common.fs import S3Config


class WriteFileResult(BaseModel):
    retry_count: int
    wasted_bytes: int
    small_chunk_count: int


class S3AsyncClient:
    def __init__(
        self,
        conf: S3Config,
        min_read_timeout_sec: float,
        network_mbit: float,
        network_buf_size: int = 8192,
        retry_limit: int = 8,
    ):
        self.__conf = conf
        self.__bucket_name = conf.bucket_name
        self.__retry_limit = retry_limit
        self.__min_read_timeout_sec = min_read_timeout_sec
        self.__network_mbit = network_mbit
        self.__network_buf_size = network_buf_size
        self.__read_timeout_threshold = 1.5
        self.__small_chunk_count_ratio = 0.9
        self.__presigned_url_expires_in = 3600

    async def head(self, key: str) -> S3ObjectInfoResponse | None:
        async with create_async_client(self.__conf) as client:
            try:
                s3_res = await client.head_object(Bucket=self.__bucket_name, Key=key)
                return S3ObjectInfoResponse.new(s3_res, key)
            except ClientError as e:
                res: Any = e.response
                if res["Error"]["Code"] == "404":
                    return None
                else:
                    raise e

    async def list(
        self,
        prefix: str,
        delimiter: str | None = None,
        next_token: str | None = None,
        max_keys: int | None = None,
    ) -> S3ListResponse:
        async with create_async_client(self.__conf) as client:
            kwargs = {"Bucket": self.__bucket_name, "Prefix": prefix}
            if delimiter is not None:
                kwargs["Delimiter"] = delimiter
            if next_token is not None:
                kwargs["ContinuationToken"] = next_token
            if max_keys is not None:
                kwargs["MaxKeys"] = max_keys

            s3_res = await client.list_objects_v2(**kwargs)
            return S3ListResponse.new(s3_res)

    async def list_all_objects(self, prefix: str, delimiter: str | None = None):
        next_token = None
        while True:
            res = await self.list(prefix, delimiter=delimiter, next_token=next_token)
            if res.contents is not None:
                for obj in res.contents:
                    yield obj
            if not res.is_truncated:
                break
            next_token = res.next_continuation_token

    async def write(self, key: str, data: bytes):
        async with create_async_client(self.__conf) as client:
            await client.put_object(Bucket=self.__bucket_name, Key=key, Body=data)

    async def read(self, key: str) -> bytes:
        async with create_async_client(self.__conf) as client:
            res = await client.get_object(Bucket=self.__bucket_name, Key=key)
            async with res["Body"] as body:
                return await body.read()

    async def generate_presigned_url(self, key: str) -> str:
        async with create_async_client(self.__conf) as client:
            return await client.generate_presigned_url(
                "get_object",
                Params={"Bucket": self.__bucket_name, "Key": key},
                ExpiresIn=self.__presigned_url_expires_in,
                HttpMethod="GET",
            )

    async def write_file(self, key: str, file_path: str, sync_time: bool = False):
        url = await self.generate_presigned_url(key)

        retry_cnt_total = 0
        wasted_sum = 0
        write_sum = 0
        small_chunk_cnt = 0
        for retry_cnt in range(self.__retry_limit + 1):
            try:
                loop = asyncio.get_event_loop()
                start = loop.time()
                write_sum = 0
                small_chunk_cnt = 0
                limiter = nio_limiter(self.__network_mbit, self.__network_buf_size)
                async with aiohttp.ClientSession() as session:
                    async with session.get(url) as res:
                        if res.status >= 400:
                            raise ValueError(f"Failed to download: status={res.status}, key={key}")
                        content_length, last_modified = _parse_get_object_res_headers(res)

                        expected_duration = content_length / (self.__network_mbit * 1024 * 1024 / 8)
                        read_timeout_sec = expected_duration * self.__read_timeout_threshold
                        if read_timeout_sec < self.__min_read_timeout_sec:
                            read_timeout_sec = self.__min_read_timeout_sec

                        async with aiofiles.open(file_path, "wb") as file:
                            while True:
                                async with limiter:
                                    chunk = await res.content.read(self.__network_buf_size)
                                    if not chunk:
                                        break

                                    size = len(chunk)
                                    write_sum += size
                                    if size < self.__network_buf_size * self.__small_chunk_count_ratio:
                                        small_chunk_cnt += 1

                                    duration = loop.time() - start
                                    if duration > read_timeout_sec:
                                        attr = f"duration={duration}, size={write_sum}, key={key}"
                                        raise TimeoutError(f"Read timeout exceeded: {attr}")

                                    await file.write(chunk)
                        if sync_time:
                            times = (last_modified.timestamp(), last_modified.timestamp())
                            await utime(file_path, times)
                break
            except Exception as e:
                if await aios.path.exists(file_path):
                    await aios.remove(file_path)

                if retry_cnt == self.__retry_limit:
                    log.error(f"Read object retry limit exceeded", _retry_error_attr(e, retry_cnt, key))
                    raise

                retry_cnt_total += 1
                wasted_sum += write_sum

        return WriteFileResult(retry_count=retry_cnt_total, wasted_bytes=wasted_sum, small_chunk_count=small_chunk_cnt)

    async def delete(self, key: str):
        for retry_cnt in range(self.__retry_limit + 1):
            try:
                async with create_async_client(self.__conf) as client:
                    await client.delete_object(Bucket=self.__bucket_name, Key=key)
                break
            except Exception as e:
                if retry_cnt == self.__retry_limit:
                    log.error(f"delete object retry limit exceeded", _retry_error_attr(e, retry_cnt, key))
                    raise
                log.warn(f"Failed to delete object", _retry_error_attr(e, retry_cnt, key))


def create_async_client(conf: S3Config) -> S3Client:
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
