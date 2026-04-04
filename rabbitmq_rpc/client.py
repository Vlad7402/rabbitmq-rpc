import asyncio
import logging
from asyncio import AbstractEventLoop, TimeoutError
from typing import Union, Callable, Any, Optional, Dict, Awaitable

from aio_pika import connect_robust, DeliveryMode, exceptions, RobustConnection

from aio_pika.abc import AbstractRobustConnection
from aio_pika.patterns import RPC, JsonRPC, Master

from rabbitmq_rpc.config import RabbitMQConfig
from rabbitmq_rpc.exceptions import MQConnectionError, RPCError, EventRegistrationError
from rabbitmq_rpc.utils import with_retry_and_timeout


class RPCClient:
    """A wrapper for aio-pika that simplifies the usage of RabbitMQ with asynchronous RPC."""

    _instances: Dict[str, 'RPCClient'] = {}
    _locks: Dict[str, asyncio.Lock] = {}

    def __init__(
            self,
            config: RabbitMQConfig,
            loop: Optional[AbstractEventLoop] = None,
            logger: Optional[logging.Logger] = None,
    ) -> None:
        """Initializes the RPCClient instance."""
        self.config = config
        self.loop = loop or asyncio.get_event_loop()
        self.logger = logger or logging.getLogger(__name__)
        self.rpc: Optional[Union[RPC, JsonRPC]] = None
        self.master: Optional[Master] = None
        self.connection: Optional[AbstractRobustConnection] = None

    @property
    def url(self) -> str:
        """Returns the RabbitMQ URL."""
        return self.config.get_url()

    @staticmethod
    async def create(
            config: Optional[RabbitMQConfig] = None,
            loop: Optional[AbstractEventLoop] = None,
            logger: Optional[logging.Logger] = None,
            host: Optional[str] = None,
            port: Optional[int] = None,
            user: Optional[str] = None,
            password: Optional[str] = None,
            vhost: Optional[str] = '/',
            ssl: bool = False,
            **kwargs,
    ) -> 'RPCClient':
        """Creates or returns an existing instance of RPCClient."""
        assert (
                (config is not None) or
                (
                        (host is not None) and
                        (port is not None) and
                        (user is not None) and
                        (password is not None)
                )
        ), "Use config or host, port, user and password to connect to RabbitMQ."
        if config is None:
            config = RabbitMQConfig(
                host=host,
                port=port,
                user=user,
                password=password,
                vhost=vhost,
                ssl=ssl
            )

        url = config.get_url()

        if url not in RPCClient._locks:
            RPCClient._locks[url] = asyncio.Lock()

        async with RPCClient._locks[url]:
            if url not in RPCClient._instances:
                loop = loop or asyncio.get_running_loop()

                if logger is None:
                    logging.basicConfig(
                        level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s'
                    )
                    logger = logging.getLogger(__name__)

                RPCClient._instances[url] = await RPCClient.__create_instance(
                    config=config,
                    logger=logger,
                    loop=loop,
                    **kwargs,
                )

        return RPCClient._instances[url]

    @staticmethod
    async def __create_instance(
            config: RabbitMQConfig,
            loop: AbstractEventLoop,
            logger: logging.Logger,
            **kwargs,
    ) -> 'RPCClient':
        """Creates an instance of RPCClient."""
        instance = RPCClient(config=config, loop=loop, logger=logger)
        await instance.connect(**kwargs)
        return instance

    @property
    def is_connected(self) -> bool:
        """Checks if the client is connected to RabbitMQ."""
        return ((self.rpc is not None and self.rpc.channel and not self.rpc.channel.is_closed)
                and (self.master is not None and self.master.channel and not self.master.channel.is_closed))

    async def connect(self, **kwargs: Any) -> None:
        """Connects to the RabbitMQ server."""
        try:
            self.connection = await connect_robust(
                url=self.url, loop=self.loop,
            )
            channel = await self.connection.channel()
            self.rpc = await RPC.create(channel, **kwargs)
            self.master = Master(channel)
            self.rpc.loop = self.loop
            self.logger.info("Connected to RabbitMQ")
        except (exceptions.AMQPConnectionError, exceptions.AMQPChannelError) as e:
            self.logger.error(f"Failed to connect to RabbitMQ at {self.url}: {str(e)}")
            raise MQConnectionError(f"Failed to connect to RabbitMQ: {str(e)}")

    async def reconnect(self, **kwargs: Any) -> None:
        """Reconnects to the RabbitMQ server."""
        try:
            await self.close()
            await self.connect(**kwargs)
            self.logger.info("Reconnected to RabbitMQ")
        except MQConnectionError as e:
            self.logger.error(f"Failed to reconnect to RabbitMQ: {str(e)}")
            raise e

    async def close(self) -> None:
        """Closes the RabbitMQ connection."""
        if self.connection:
            try:
                await self.connection.close()
                self.rpc = None
                self.connection = None
                self.logger.info("Closed RabbitMQ connection")
            except exceptions.AMQPError as e:
                self.logger.error(f"Failed to close RabbitMQ connection: {str(e)}")
                raise MQConnectionError(f"Failed to close RabbitMQ connection: {str(e)}")

    def set_event_loop(self, loop: AbstractEventLoop) -> None:
        """Sets the event loop for the RPC client."""
        self.loop = loop
        if self.rpc:
            self.rpc.loop = loop

    def set_logger(self, logger: logging.Logger) -> None:
        """Sets the logger for the RPC client."""
        self.logger = logger

    def get_logger(self) -> logging.Logger:
        """Returns the logger."""
        return self.logger

    def get_connection(self) -> Optional[RobustConnection]:
        """Returns the current connection."""
        return self.connection

    async def send(
            self,
            event: str,
            retry_count: int = 1,
            timeout: float = 15,
            **kwargs: Any,
    ) -> None:
        """Sends an event with the specified parameters. Kwargs support only"""
        await self._send_event(
            event,
            "worker",
            retry_count,
            timeout,
            self.master.create_task(
            channel_name=event,
            kwargs=kwargs)
        )

    async def call(
            self,
            event: str,
            retry_count: Optional[int] = 1,
            timeout: Optional[float] = 15,
            expiration: Optional[int] = None,
            priority: int = 5,
            delivery_mode: DeliveryMode = DeliveryMode.PERSISTENT,
            **kwargs: Any,
    ) -> Any:
        """Calls an RPC method with the specified parameters. Kwargs support only"""
        result = await self._send_event(
            event,
            "RPC",
            retry_count,
            timeout,
            self.rpc.call(
                method_name=event,
                kwargs=kwargs,
                expiration=expiration,
                priority=priority,
                delivery_mode=delivery_mode)
        )
        self.logger.info(f"RPC call {event} succeed")
        return result

    async def _send_event(
            self,
            event: str,
            event_type_str: str,
            retry_count: int,
            timeout: float,
            coro: Awaitable
    ) -> Any:
        """Calls a method via RabbitMQ"""
        if not self.is_connected:
            raise MQConnectionError("RPCClient is not connected")
        try:
            self.logger.info(f"{event_type_str} call {event}")
            return await with_retry_and_timeout(coro, timeout=timeout, retry_count=retry_count)
        except (TimeoutError, exceptions.AMQPError) as e:
            self.logger.error(f"Failed to call {event_type_str} {event}: {str(e)}")
            raise RPCError(f"Failed to call {event_type_str} {event}: {str(e)}")

    async def register_rpc_callable(self, event: str, handler: Callable[..., Any], **kwargs: Any) -> None:
        """Registers an event handler for RPC calls."""
        await self._register_handler(
            event,
            "RPC",
            self.rpc.register(
                method_name=event,
                func=handler,
                auto_delete=True,
                **kwargs)
        )

    async def register_worker(self, event: str, handler: Callable[..., Any], **kwargs: Any) -> None:
        """Registers an event handler for worker. Will not send back a response."""
        await self._register_handler(
            event,
            "worker",
            self.master.create_worker(
                queue_name=event,
                func=handler,
                auto_delete=True,
                **kwargs)
        )

    async def _register_handler(self, event: str, event_type: str, coro: Awaitable) -> None:
        """Registers an event handler."""
        if not self.is_connected:
            raise MQConnectionError("RPCClient is not connected")
        try:
            await coro
            self.logger.info(f"Registered {event_type} for {event}")
        except (exceptions.AMQPError, ValueError) as e:
            self.logger.error(f"Failed to register {event_type} for {event}: {str(e)}")
            raise EventRegistrationError(f"Failed to register {event_type} for {event}: {str(e)}")

    def __repr__(self) -> str:
        """Returns a string representation of the RPCClient instance."""
        return f"RPCClient(config={self.config})"

    def __str__(self) -> str:
        """Returns a string representation of the RPCClient instance."""
        return self.__repr__()
