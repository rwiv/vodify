from pydantic import BaseModel, constr, conint


class RedisConfig(BaseModel):
    host: constr(min_length=1)
    port: conint(ge=1)
    password: constr(min_length=1)
