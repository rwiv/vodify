import os
import sys

from pyutils import load_dotenv, path_join, find_project_root, log

if __name__ == "__main__":
    if len(sys.argv) < 2:
        log.info("Usage: python -m vtask [worker|server|batch]")
        sys.exit(1)

    mode = sys.argv[1]

    if mode == "worker":
        from .celery import run

        run()
    elif mode == "server":
        env = os.getenv("PY_ENV") or None
        if env == "dev" or env is None:
            log.info("Loading .env-server-dev")
            load_dotenv(path_join(find_project_root(), "dev", ".env-server-dev"))

        from .server import run

        run()
    elif mode == "batch":
        from .common.batch import BatchRunner

        BatchRunner().run()
    else:
        log.info(f"Unknown mode: {mode}")
        sys.exit(1)
