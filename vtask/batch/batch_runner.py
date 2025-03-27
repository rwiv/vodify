from ..common.env import get_batch_env
from ..common.env.env_batch import BatchCommand
from ..service.loss import LossExecutor


class BatchRunner:
    def run(self):
        env = get_batch_env()
        if env.command == BatchCommand.LOSS:
            LossExecutor(env).run()
        else:
            raise ValueError(f"Unknown command: {env.command}")
