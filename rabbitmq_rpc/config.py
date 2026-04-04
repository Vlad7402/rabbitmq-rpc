from dataclasses import dataclass
from typing import Optional

from aio_pika.connection import make_url

@dataclass
class RabbitMQConfig:
    host: Optional[str] = "localhost"
    port: Optional[int] = 5672
    user: Optional[str] = "admin"
    password: Optional[str] = "secure_password"
    vhost: Optional[str] = "/"
    ssl: Optional[bool] = False

    def get_url(self) -> str:
        return str(
            make_url(
                host=self.host,
                port=self.port,
                login=self.user,
                password=self.password,
                virtualhost=self.vhost,
                ssl=self.ssl
            )
        )
