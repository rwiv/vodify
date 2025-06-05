import asyncio
import os
import shutil
import tarfile
from pathlib import Path

from aiofiles import os as aios
from pyutils import path_join


def stem(file_path: str) -> str:
    return Path(file_path).stem


def check_dir(base_path: str):
    dir_path = Path(base_path).resolve().parent
    if not dir_path.exists():
        os.makedirs(dir_path, exist_ok=True)


async def ensure_dir(dir_path: str):
    if not await aios.path.exists(dir_path):
        await aios.makedirs(dir_path, exist_ok=True)
    return dir_path


async def listdir_recur(dir_path: str) -> list[str]:
    return await asyncio.to_thread(read_dir_recur, dir_path)


def read_dir_recur(dir_path: str):
    paths = []
    for root, _, files in os.walk(dir_path):
        for file in files:
            paths.append(os.path.join(root, file))
    return paths


async def move_directory_not_recur(src_dir_path: str, dst_dir_path: str):
    move_start = asyncio.get_event_loop().time()
    await aios.makedirs(dst_dir_path, exist_ok=True)
    for file_path in await aios.listdir(src_dir_path):
        if await aios.path.isdir(file_path):
            raise ValueError("only supports files")
        src = path_join(src_dir_path, file_path)
        dst = path_join(dst_dir_path, file_path)
        await move_file(src=src, dst=dst)
    return asyncio.get_event_loop().time() - move_start


async def rmtree(dir_path: str):
    await asyncio.to_thread(shutil.rmtree, path=dir_path)


async def move_file(src: str, dst: str):
    await asyncio.to_thread(shutil.move, src=src, dst=dst)


async def copy_file(src: str, dst: str):
    await asyncio.to_thread(shutil.copy, src=src, dst=dst)


async def utime(path: str, times: tuple[float, float]):
    await asyncio.to_thread(os.utime, path, times)


def _open_tar(tar_path: str, out_dir_path: str):
    with tarfile.open(tar_path, "r:*") as tar:
        tar.extractall(path=out_dir_path)


async def open_tar(tar_path: str, out_dir_path: str):
    await asyncio.to_thread(_open_tar, tar_path, out_dir_path)
