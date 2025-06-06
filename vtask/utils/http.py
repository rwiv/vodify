from typing import Any

import aiohttp

user_agent = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)


def get_headers(cookie_str: str | None = None, accept: str | None = None) -> dict:
    headers = {
        "User-Agent": user_agent,
    }
    if accept is not None:
        headers["Accept"] = accept
    if cookie_str is not None:
        headers["Cookie"] = cookie_str
    return headers


async def fetch_text(url: str, headers: dict | None = None) -> str:
    async with aiohttp.ClientSession(headers=headers) as session:
        async with session.get(url) as res:
            if res.status >= 400:
                raise ValueError(f"Failed to request: {res.status}")
            return await res.text()


async def fetch_json(url: str, headers: dict | None = None) -> Any:
    async with aiohttp.ClientSession(headers=headers) as session:
        async with session.get(url) as res:
            if res.status >= 400:
                raise ValueError(f"Failed to request: {res.status}")
            return await res.json()
