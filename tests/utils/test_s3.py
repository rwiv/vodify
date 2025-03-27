import os

from pyutils import find_project_root, path_join

from vtask.common.fs import read_fs_config, FsConfig
from vtask.utils import S3Client, S3ListResponse

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
s3 = S3Client(s3_conf)


def test_s3():
    print()

    set_environment()

    prefix = "a2/"
    delimiter = "/"
    # delimiter = None
    max_keys = 2

    res1 = s3.list(prefix=prefix, delimiter=delimiter, max_keys=max_keys)
    print_list(res1)
    res2 = s3.list(
        prefix=prefix, delimiter=delimiter, max_keys=max_keys, next_token=res1.next_continuation_token
    )
    print_list(res2)
    res3 = s3.list(
        prefix=prefix, delimiter=delimiter, max_keys=max_keys, next_token=res2.next_continuation_token
    )
    print_list(res3)

    # for f in s3.list_all_objects(prefix=prefix, delimiter=delimiter, max_keys=max_keys):
    #     print(f.key)

    clear_all()


def test_stream_write():
    big_file_path = path_join(find_project_root(), "dev", "big_file.txt")
    with open(big_file_path, "wb") as f:
        f.write(b"a" * 1024 * 1024 * 10)
    with open(big_file_path, "rb") as f:
        s3.write("/a/test1.txt", f)
    body = s3.read("/a/test1.txt")
    s3.write("/a/test2.txt", body)  # type: ignore
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


def clear_all():
    res = s3.list("")
    if res.contents is None:
        return
    for f in res.contents:
        s3.delete(f.key)


def set_environment():
    s3.write("a1/test1.txt", b"test")
    s3.write("a1/test2.txt", b"test")

    s3.write("a2/test1.txt", b"test")
    s3.write("a2/test2.txt", b"test")

    s3.write("a1/b1/test1.txt", b"test")
    s3.write("a1/b1/test2.txt", b"test")
    s3.write("a1/b2/test1.txt", b"test")
    s3.write("a1/b2/test2.txt", b"test")

    s3.write("a2/b1/test1.txt", b"test")
    s3.write("a2/b1/test2.txt", b"test")
    s3.write("a2/b2/test1.txt", b"test")
    s3.write("a2/b2/test2.txt", b"test")
    s3.write("a2/b3/test1.txt", b"test")
