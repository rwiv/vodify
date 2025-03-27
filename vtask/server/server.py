import uvicorn
from fastapi import FastAPI

from .server_deps import deps


def run():
    env = deps.env

    app = FastAPI()

    deps.stdl_listener.run()

    app.include_router(deps.celery_router)
    app.include_router(deps.stdl_router)

    uvicorn.run(app, port=env.server.port, host="0.0.0.0")
