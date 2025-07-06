from aiohttp_socks import ProxyConnector, ProxyType
from pydantic import BaseModel, constr, conint


class ProxyConfig(BaseModel):
    host: constr(min_length=1)
    port: conint(ge=0)
    username: constr(min_length=1)
    password: constr(min_length=1)
    rdns: bool

    def proxy_connector(self):
        return ProxyConnector(
            proxy_type=ProxyType.SOCKS5,
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            rdns=self.rdns,
        )
