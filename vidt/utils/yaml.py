from typing import Any

import aiofiles
import yaml


async def write_yaml_file(data: Any, out_file_path: str):
    async with aiofiles.open(out_file_path, "w") as file:
        await file.write(yaml.dump(data, allow_unicode=True))
