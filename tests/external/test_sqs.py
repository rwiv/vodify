import pytest

from tests.testutils.test_utils_misc import load_test_dotenv
from vidt.env import get_server_env
from vidt.external.sqs import SQSAsyncClient

load_test_dotenv(".env-server-dev")
# load_test_dotenv(".env-worker-prod")

env = get_server_env()
client = SQSAsyncClient(env.sqs)


@pytest.mark.asyncio
async def test_send():
    for i in range(2):
        await client.send(f"test{i}")


@pytest.mark.asyncio
async def test_receive():
    messages = await client.receive()
    print([msg.body for msg in messages])
    await client.delete(messages)
