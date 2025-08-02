import pytest

from vidt.video import ChzzkVideoClient1


@pytest.mark.asyncio
async def test_get_info():
    print()
    title_no = 0
    client = ChzzkVideoClient1(cookie_str=None)
    info = await client.get_video_info(title_no)
    print(info)
