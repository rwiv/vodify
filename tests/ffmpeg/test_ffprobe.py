import pytest
from pyutils import path_join, find_project_root

from vtask.ffmpeg import get_info


@pytest.mark.asyncio
async def test_get_info():
    vid_path = path_join(find_project_root(), "dev", "test", "assets", "enc", "test.mp4")
    info = await get_info(vid_path)
    assert "test.mp4" in info.format.filename
