import asyncio
import logging
from asyncio import AbstractEventLoop, TimeoutError
from typing import Union, Callable, Any, Optional, Awaitable, List

from aio_pika import connect_robust, DeliveryMode, exceptions, RobustConnection

from aio_pika.abc import AbstractRobustConnection
from aio_pika.patterns import RPC, JsonRPC, Master

from rabbitmq_rpc.config import RabbitMQConfig
from rabbitmq_rpc.exceptions import MQConnectionError, RPCError, EventRegistrationError, RPCClientException
from rabbitmq_rpc.utils import AsyncMixin, get_log_handler


class RPCClient(AsyncMixin):
    """A wrapper for aio-pika that simplifies the usage of RabbitMQ with asynchronous RPC."""

    _instance: Optional[RPCClient] = None

    @staticmethod
    def get_instance() -> RPCClient:
        if RPCClient._instance is None: raise RPCClientException("Instance not created.")
        return RPCClient._instance

    def __init__(
            self,
            service_name: str,
            config: Optional[RabbitMQConfig] = None,
            loop: Optional[AbstractEventLoop] = None,
            logger: Optional[logging.Logger] = None,
            host: Optional[str] = None,
            port: Optional[int] = None,
            user: Optional[str] = None,
            password: Optional[str] = None,
            vhost: Optional[str] = '/',
            ssl: bool = False,
    ) -> None:
        """Creates or returns an existing instance of RPCClient."""
        self.service_name = service_name
        self.__registered_handlers_names: List[str] = []
        self.config = config
        self.loop = loop or asyncio.get_event_loop()
        self.logger = logger or logging.getLogger(__name__)
        self.rpc: Optional[Union[RPC, JsonRPC]] = None
        self.master: Optional[Master] = None
        self.connection: Optional[AbstractRobustConnection] = None

        if RPCClient._instance is not None: raise RPCClientException("Only one instance is allowed.")

        if(
            (config is None) and
            (
                (host is None) or
                (port is None) or
                (user is None) or
                (password is None)
            )
        ): raise RPCClientException("Use config or host, port, user and password to connect to RabbitMQ.")

        if config is None:
            self.config = RabbitMQConfig(
                host=host,
                port=port,
                user=user,
                password=password,
                vhost=vhost,
                ssl=ssl
            )

        if logger is None:
            self.logger = logging.getLogger("RCP_Client")
            self.logger.setLevel(logging.INFO)
            self.logger.addHandler(get_log_handler())

        RPCClient._instance = self
        super().__init__()
        if not self.async_initialized: raise RPCClientException("Use rpc_client = await RPCClient()."
                                                                " Await is necessary.")


    async def __ainit__(self, *args, **kwargs):
        """Initializes the RPCClient instance."""
        if not self.is_connected: await self.connect()
        self.async_initialized = True

    @property
    def url(self) -> str:
        """Returns the RabbitMQ URL."""
        return self.config.get_url()

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
            service_name: str,
            event: str,
            timeout: float = 15,
            **kwargs: Any,
    ) -> None:
        """Sends an event with the specified parameters. Kwargs support only"""
        await self._send_event(
            f"{service_name}.{event}",
            "worker",
            timeout,
            self.master.create_task(
            channel_name=f"{service_name}.{event}_worker",
            kwargs=kwargs)
        )

    async def call(
            self,
            service_name: str,
            event: str,
            timeout: Optional[float] = 15,
            expiration: Optional[int] = None,
            priority: int = 5,
            delivery_mode: DeliveryMode = DeliveryMode.PERSISTENT,
            **kwargs: Any,
    ) -> Any:
        """Calls an RPC method with the specified parameters. Kwargs support only"""
        result = await self._send_event(
            f"{service_name}.{event}",
            "RPC",
            timeout,
            self.rpc.call(
                method_name=f"{service_name}.{event}_RPC",
                kwargs=kwargs,
                expiration=expiration,
                priority=priority,
                delivery_mode=delivery_mode)
        )
        self.logger.info(f"RPC call {service_name}.{event} succeed")
        return result

    async def _send_event(
            self,
            event: str,
            event_type_str: str,
            timeout: float,
            coro: Awaitable
    ) -> Any:
        """Calls a method via RabbitMQ"""
        if not self.is_connected:
            raise MQConnectionError("RPCClient is not connected")
        try:
            self.logger.info(f"{event_type_str} call {event}")
            return await asyncio.wait_for(coro, timeout=timeout)
        except TimeoutError as e:
            self.logger.error(f"Timeout {timeout:.1f} sec reached on call {event_type_str} {event}: {e}")
            raise RPCError(f"Timeout {timeout:.1f} sec reached on call {event_type_str} {event}: {e}")
        except exceptions.AMQPError as e:
            self.logger.error(f"Failed to call {event_type_str} {event}: {e}")
            raise RPCError(f"Failed to call {event_type_str} {event}: {e}")

    async def register_rpc_callable(self, event: str, handler: Callable[..., Any], **kwargs: Any) -> None:
        """Registers an event handler for RPC calls."""
        await self._register_handler(
            event,
            "RPC",
            self.rpc.register(
                method_name=f"{self.service_name}.{event}_RPC",
                func=self.__logged_handler(handler, event, "RPC"),
                auto_delete=True,
                **kwargs)
        )

    async def register_worker(self, event: str, handler: Callable[..., Any], **kwargs: Any) -> None:
        """Registers an event handler for worker. Will not send back a response."""
        await self._register_handler(
            event,
            "worker",
            self.master.create_worker(
                queue_name=f"{self.service_name}.{event}_worker",
                func=self.__logged_handler(handler, event, "worker"),
                auto_delete=True,
                **kwargs)
        )

    async def _register_handler(self, event: str, event_type: str, coro: Awaitable) -> None:
        """Registers an event handler."""
        name_used = False
        try:
            self.__registered_handlers_names.index(f"{self.service_name}.{event}_{event_type}")
            name_used = True
        except ValueError:
            pass
        if name_used: raise EventRegistrationError(f"Event with name {event} already registered")
        if not self.is_connected:
            raise MQConnectionError("RPCClient is not connected")
        try:
            await coro
            self.__registered_handlers_names.append(f"{self.service_name}.{event}_{event_type}")
            self.logger.info(f"Registered {event_type} for {event} on service {self.service_name}")
        except (exceptions.AMQPError, ValueError) as e:
            self.logger.error(f"Failed to register {event_type} for {event} on service {self.service_name}: {str(e)}")
            raise EventRegistrationError(f"Failed to register {event_type} for {event}"
                                         f" on service {self.service_name}: {str(e)}")

    def __logged_handler(self, handler: Callable[..., Any], event: str, event_type: str):
        """Logging funk for calls"""

        def new_handler(*args, **kwargs):
            nonlocal handler
            nonlocal event
            self.logger.info(f"Handler {event_type} {event} of service {self.service_name} called")
            return handler(*args, **kwargs)

        return new_handler

    def __repr__(self) -> str:
        """Returns a string representation of the RPCClient instance."""
        return f"RPCClient(config={self.config})"

    def __str__(self) -> str:
        """Returns a string representation of the RPCClient instance."""
        return self.__repr__()
