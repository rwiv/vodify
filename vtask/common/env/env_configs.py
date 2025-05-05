import os

from pydantic import BaseModel, constr


class UntfConfig(BaseModel):
    endpoint: constr(min_length=1)
    api_key: constr(min_length=1)
    topic: constr(min_length=1)


def read_untf_env():
    return UntfConfig(
        endpoint=os.getenv("UNTF_ENDPOINT"),
        api_key=os.getenv("UNTF_API_KEY"),
        topic=os.getenv("UNTF_TOPIC"),
    )
