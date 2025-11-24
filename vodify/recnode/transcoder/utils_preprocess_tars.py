from pathlib import Path

from aiofiles import os as aios
from pyutils import path_join

from ...utils import ensure_dir, listdir_recur, open_tar, stem


async def _validate_tar_files(tars_dir_path: str):
    if not Path(tars_dir_path).exists():
        raise ValueError(f"Source path {tars_dir_path} does not exist.")
    tar_names = await aios.listdir(tars_dir_path)
    if len(tar_names) == 0:
        raise ValueError(f"Source path {tars_dir_path} is empty.")
    for tar_name in tar_names:
        if not tar_name.endswith(".tar"):
            raise ValueError(f"Invalid file ext: {path_join(tars_dir_path, tar_name)}")


async def _extract_tar_files(src_dir_path: str, out_dir_path: str) -> list[str]:
    for tar_filename in await aios.listdir(src_dir_path):
        extracted_dir_path = await ensure_dir(path_join(out_dir_path, stem(tar_filename)))
        await open_tar(path_join(src_dir_path, tar_filename), extracted_dir_path)

    extracted_seg_paths = []
    for file_path in await listdir_recur(out_dir_path):
        if not file_path.endswith(".ts"):
            raise ValueError(f"Invalid file ext: {file_path}")
        extracted_seg_paths.append(file_path)

    return extracted_seg_paths
