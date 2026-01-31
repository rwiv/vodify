import asyncio
from datetime import datetime

import pytest
from pyutils import find_project_root, path_join

from vodify.common.fs import read_fs_config, FsConfig
from vodify.external.s3 import S3ListResponse, create_client

fs_configs = read_fs_config(path_join(find_project_root(), "dev", "fs_conf_test.yaml"))
# fs_configs = read_fs_config(path_join(find_project_root(), "dev", "fs_conf.yaml"))

fs_name = "minio"
PLATFORM = "chzzk"
BASE_DATE = datetime(2026, 1, 29)

fs_conf: FsConfig | None = None
for conf in fs_configs:
    if conf.name == fs_name:
        fs_conf = conf
        break

if fs_conf is None:
    raise ValueError("FS config not found")
s3_conf = fs_conf.s3


async def list_objects(
    prefix: str,
    delimiter: str,
    max_keys: int | None = None,
) -> S3ListResponse:
    if s3_conf is None:
        raise ValueError("S3 config not found")
    async with create_client(s3_conf) as client:
        kwargs = {
            "Bucket": s3_conf.bucket_name,
            "Prefix": prefix,
            "Delimiter": delimiter,
        }
        if max_keys is not None:
            kwargs["MaxKeys"] = max_keys  # type: ignore

        s3_res = await client.list_objects_v2(**kwargs)
        return S3ListResponse.new(s3_res)


@pytest.mark.asyncio
async def test_s3():
    print()
    prefix = path_join("incomplete", PLATFORM + "/")
    res = await list_objects(prefix=prefix, delimiter="/", max_keys=1000)
    if res.contents is not None or res.prefixes is None:
        raise ValueError(f"Unexpected response: {res}")

    prefixes = [chan_p.prefix for chan_p in res.prefixes]
    batch_size = 10
    vid_prefixes = []
    for i in range(0, len(prefixes), batch_size):
        batch_prefixes = prefixes[i : i + batch_size]
        vid_prefiex_batch = await fetch_batch_vid_prefixes(prefixes=batch_prefixes)
        vid_prefixes.extend(vid_prefiex_batch)
        print(f"Batch {i // batch_size + 1} done")

    for vid_prefiex in vid_prefixes:
        channel_name = vid_prefiex.split("/")[-3]
        video_name = vid_prefiex.split("/")[-2]

        date = datetime.strptime(video_name, "%y%m%d_%H%M%S")
        if date < BASE_DATE:
            print(f"{channel_name}/{video_name}")


async def fetch_batch_vid_prefixes(prefixes: list[str]) -> list[str]:
    coroutines = [fetch_vid_prefixes(prefix=prefix) for prefix in prefixes]
    vid_prefix_lists = await asyncio.gather(*coroutines)
    return [vid_prefix for vid_prefixes in vid_prefix_lists for vid_prefix in vid_prefixes]


async def fetch_vid_prefixes(prefix: str) -> list[str]:
    res = await list_objects(prefix=prefix, delimiter="/")
    if res.contents is not None or res.prefixes is None:
        raise ValueError(f"Unexpected response: {res}")
    return [vid_p.prefix for vid_p in res.prefixes]
