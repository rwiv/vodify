from aiolimiter import AsyncLimiter


def nio_limiter(mbit: float, buf_size: int) -> AsyncLimiter:
    return AsyncLimiter((mbit * 1024 * 1024 / 8) / buf_size, 1)
