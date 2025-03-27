import time
from io import BufferedReader
from typing import Any

from botocore.exceptions import ClientError
from botocore.response import StreamingBody
from pyutils import log, error_dict

from .s3_responses import S3ListResponse, S3ObjectInfoResponse
from .s3_utils import create_client
from ..common.fs import S3Config


class S3Client:
    def __init__(self, conf: S3Config, retry_limit: int = 3):
        self.config = conf
        self.bucket_name = conf.bucket_name
        self.__s3 = create_client(conf)
        self.retry_limit = retry_limit

    def head(self, key: str) -> S3ObjectInfoResponse | None:
        try:
            s3_res = self.__s3.head_object(Bucket=self.bucket_name, Key=key)
            return S3ObjectInfoResponse.new(s3_res, key)
        except ClientError as e:
            res: Any = e.response
            if res["Error"]["Code"] == "404":
                return None
            else:
                raise e

    def list(
        self,
        prefix: str,
        delimiter: str | None = None,
        next_token: str | None = None,
        max_keys: int | None = None,
    ) -> S3ListResponse:
        kwargs = {"Bucket": self.bucket_name, "Prefix": prefix}
        if delimiter is not None:
            kwargs["Delimiter"] = delimiter
        if next_token is not None:
            kwargs["ContinuationToken"] = next_token
        if max_keys is not None:
            kwargs["MaxKeys"] = max_keys

        s3_res = self.__s3.list_objects_v2(**kwargs)
        return S3ListResponse.new(s3_res)

    def list_all_objects(self, prefix: str, delimiter: str | None = None):
        next_token = None
        while True:
            res = self.list(prefix, delimiter=delimiter, next_token=next_token)
            if res.contents is not None:
                for obj in res.contents:
                    yield obj
            if not res.is_truncated:
                break
            next_token = res.next_continuation_token

    def read(self, key: str) -> StreamingBody:
        res = self.__s3.get_object(Bucket=self.bucket_name, Key=key)
        return res["Body"]

    def write(self, key: str, data: bytes | BufferedReader):
        if isinstance(data, bytes):
            self.__s3.put_object(Bucket=self.bucket_name, Key=key, Body=data)
        elif isinstance(data, BufferedReader):
            self.__s3.upload_fileobj(data, self.bucket_name, key)
        else:
            raise ValueError(f"Unsupported data type: {type(data)}")

    def write_file(
        self,
        key: str,
        file_path: str,
        network_io_delay_ms: int,
        network_buf_size: int,
    ):
        for retry_cnt in range(self.retry_limit + 1):
            try:
                with self.read(key) as body:
                    with open(file_path, "wb") as f:
                        while True:
                            chunk = body.read(network_buf_size)
                            if not chunk:
                                break
                            f.write(chunk)
                            if network_io_delay_ms > 0:
                                time.sleep(network_io_delay_ms / 1000)
                break
            except Exception as e:
                if retry_cnt == self.retry_limit:
                    log.error(f"Read object retry limit exceeded", _retry_error_attr(e, retry_cnt, key))
                    raise
                log.warn(f"Failed to read object", _retry_error_attr(e, retry_cnt, key))

    def delete(self, key: str):
        for retry_cnt in range(self.retry_limit + 1):
            try:
                self.__s3.delete_object(Bucket=self.bucket_name, Key=key)
                break
            except Exception as e:
                if retry_cnt == self.retry_limit:
                    log.error(f"delete object retry limit exceeded", _retry_error_attr(e, retry_cnt, key))
                    raise
                log.warn(f"Failed to delete object", _retry_error_attr(e, retry_cnt, key))


def _retry_error_attr(ex: Exception, retry_cnt: int, key: str) -> dict[str, Any]:
    attr = error_dict(ex)
    attr["cnt"] = retry_cnt
    attr["key"] = key
    return attr
