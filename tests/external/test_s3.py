import asyncio
import os

import pytest
from pyutils import find_project_root, path_join

from vidt.common.fs import read_fs_config, FsConfig
from vidt.external.s3 import S3ListResponse, S3AsyncClient
from vidt.utils import cur_duration

fs_configs = read_fs_config(path_join(find_project_root(), "dev", "fs_conf_test.yaml"))

fs_name = "minio"

fs_conf: FsConfig | None = None
for conf in fs_configs:
    if conf.name == fs_name:
        fs_conf = conf
        break

if fs_conf is None:
    raise ValueError("FS config not found")
s3_conf = fs_conf.s3
if s3_conf is None:
    raise ValueError("S3 config not found")
s3 = S3AsyncClient(
    conf=s3_conf,
    network_mbit=64,
    network_buf_size=8192,
    retry_limit=1,
    min_read_timeout_sec=10,
    read_timeout_threshold=2.0,
)


@pytest.mark.asyncio
async def test_download():
    key = ""
    file_path = path_join(find_project_root(), "dev", "out.tar")
    start = asyncio.get_event_loop().time()
    await s3.write_file(key=key, file_path=file_path, sync_time=True)
    print(f"{cur_duration(start):.2f} seconds")


@pytest.mark.asyncio
async def test_s3():
    print()

    await set_environment()

    prefix = "a2/"
    delimiter = "/"
    # delimiter = None
    max_keys = 2

    res1 = await s3.list(prefix=prefix, delimiter=delimiter, max_keys=max_keys)
    print_list(res1)
    res2 = await s3.list(
        prefix=prefix,
        delimiter=delimiter,
        max_keys=max_keys,
        next_token=res1.next_continuation_token,
    )
    print_list(res2)
    res3 = await s3.list(
        prefix=prefix,
        delimiter=delimiter,
        max_keys=max_keys,
        next_token=res2.next_continuation_token,
    )
    print_list(res3)

    # for f in s3.list_all_objects(prefix=prefix, delimiter=delimiter, max_keys=max_keys):
    #     print(f.key)

    await clear_all()


@pytest.mark.asyncio
async def test_stream_write():
    big_file_path = path_join(find_project_root(), "dev", "big_file.txt")
    b = b"a" * 1024 * 1024 * 10
    src_key = "/a/test1.txt"
    await s3.write(src_key, b)
    await s3.write_file(key=src_key, file_path=big_file_path, sync_time=True)
    await s3.delete(src_key)
    os.remove(big_file_path)


def print_list(res: S3ListResponse):
    print("--------list--------")
    print(res.key_count)
    print(res.is_truncated)
    print(res.next_continuation_token)
    print("--------prefixes--------")
    if res.prefixes:
        for p in res.prefixes:
            print(p)
    print("--------contents--------")
    if res.contents:
        for f in res.contents:
            print(f.key)


async def clear_all():
    res = await s3.list("")
    if res.contents is None:
        return
    await s3.delete_batch([f.key for f in res.contents])


async def set_environment():
    await asyncio.gather(
        s3.write("a1/test1.txt", b"test"),
        s3.write("a1/test2.txt", b"test"),
        s3.write("a2/test1.txt", b"test"),
        s3.write("a2/test2.txt", b"test"),
        s3.write("a1/b1/test1.txt", b"test"),
        s3.write("a1/b1/test2.txt", b"test"),
        s3.write("a1/b2/test1.txt", b"test"),
        s3.write("a1/b2/test2.txt", b"test"),
        s3.write("a2/b1/test1.txt", b"test"),
        s3.write("a2/b1/test2.txt", b"test"),
        s3.write("a2/b2/test1.txt", b"test"),
        s3.write("a2/b2/test2.txt", b"test"),
        s3.write("a2/b3/test1.txt", b"test"),
    )
