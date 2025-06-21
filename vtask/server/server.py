import logging

import uvicorn
from fastapi import FastAPI
from pyutils import log

from .server_deps import ServerDependencyManager


def run_server():
    log.set_level(logging.DEBUG)

    deps = ServerDependencyManager()

    app = FastAPI()

    app.include_router(deps.default_router)
    app.include_router(deps.celery_router)
    app.include_router(deps.stdl_router)

    deps.stdl_consume_cron.start()
    deps.stdl_register_cron.start()

    uvicorn.run(app, port=deps.server_env.server.port, host="0.0.0.0", access_log=False)
