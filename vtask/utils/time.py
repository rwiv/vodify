import asyncio


def cur_duration(start_time: float) -> float:
    return asyncio.get_event_loop().time() - start_time
