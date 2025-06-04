import asyncio
import os
from datetime import datetime
from typing import Any

import aiofiles
import aiohttp
from aiobotocore.config import AioConfig
from aiobotocore.session import get_session
from botocore.exceptions import ClientError
from pyutils import log, error_dict
from types_aiobotocore_s3.client import S3Client

from .s3_responses import S3ListResponse, S3ObjectInfoResponse
from ..common.fs import S3Config


class S3AsyncClient:
    def __init__(self, conf: S3Config, retry_limit: int = 8):
        self.__conf = conf
        self.__bucket_name = conf.bucket_name
        self.__retry_limit = retry_limit
        self.__presigned_url_expires_in = 3600
        self.__read_timeout_sec = 10

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

    async def generate_presigned_url(self, key: str) -> str:
        async with create_async_client(self.__conf) as client:
            return await client.generate_presigned_url(
                "get_object",
                Params={"Bucket": self.__bucket_name, "Key": key},
                ExpiresIn=self.__presigned_url_expires_in,
                HttpMethod="GET",
            )

    async def write_file(
        self,
        key: str,
        file_path: str,
        network_io_delay_ms: float,
        network_buf_size: int,
        sync_time: bool = False,
    ):
        url = await self.generate_presigned_url(key)
        for retry_cnt in range(self.__retry_limit + 1):
            try:
                loop = asyncio.get_event_loop()
                start = loop.time()
                cnt = 0
                async with aiohttp.ClientSession() as session:
                    async with session.get(url) as res:
                        if res.status >= 400:
                            raise ValueError(f"Failed to download: key={key}, status={res.status}")
                        last_modified_str = res.headers.get("Last-Modified")
                        if not isinstance(last_modified_str, str):
                            raise ValueError(f"Invalid Last-Modified header: {last_modified_str}")
                        last_modified = datetime.strptime(last_modified_str, "%a, %d %b %Y %H:%M:%S GMT")
                        async with aiofiles.open(file_path, "wb") as file:
                            while True:
                                chunk = await res.content.read(network_buf_size)
                                if not chunk:
                                    break
                                if loop.time() - start > self.__read_timeout_sec:
                                    raise TimeoutError(f"Read timeout exceeded: key={key}")
                                if len(chunk) < network_buf_size:
                                    cnt += 1
                                await file.write(chunk)
                                if network_io_delay_ms > 0:
                                    await asyncio.sleep(network_io_delay_ms / 1000)
                        if sync_time:
                            os.utime(file_path, (last_modified.timestamp(), last_modified.timestamp()))
                # print(cnt)
                break
            except Exception as e:
                if retry_cnt == self.__retry_limit:
                    log.error(f"Read object retry limit exceeded", _retry_error_attr(e, retry_cnt, key))
                    raise
                log.warn(f"Failed to read object", _retry_error_attr(e, retry_cnt, key))

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
