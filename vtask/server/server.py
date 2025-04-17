import uvicorn
from fastapi import FastAPI

from .server_deps import ServerDependencyManager


def run():
    deps = ServerDependencyManager()
    env = deps.env

    app = FastAPI()

    app.include_router(deps.celery_router)
    app.include_router(deps.stdl_router)

    deps.stdl_cron.start()

    uvicorn.run(app, port=env.server.port, host="0.0.0.0")
