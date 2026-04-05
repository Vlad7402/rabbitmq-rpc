import asyncio
import sys

from rabbitmq_rpc import RPCClient, RabbitMQConfig


async def some_func(conf: RabbitMQConfig, need_sleep: bool):
    print(conf)
    if need_sleep: await asyncio.sleep(10)
    return conf

async def callback():
    rpc_client = RPCClient.get_instance()
    print("Server may be a client at the same time.")
    await rpc_client.send('test_client', 'callback')

async def killer():
    sys.exit(1)


async def main():
    # Initialize RPC client
    rpc_client: RPCClient = await RPCClient(
        service_name='test_server',
        host='localhost',
        port=5672,
        user='admin',
        password='secure_password',
        ssl=False,
    )

    # Register event handlers
    await rpc_client.register_rpc_callable('test', some_func)
    await rpc_client.register_worker('test', some_func)
    await rpc_client.register_worker('callback', callback)
    await rpc_client.register_rpc_callable('killer', killer)

    # Keep listening for events
    await asyncio.Future()


if __name__ == '__main__':
    asyncio.run(main())