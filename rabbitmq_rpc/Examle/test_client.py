import asyncio

from rabbitmq_rpc import RPCClient, RabbitMQConfig

def some_func():
    print('Client may be a server.')

async def main():
    # Initialize RPC client
    rpc_client = await RPCClient(
        host='localhost',
        port=5672,
        user='admin',
        password='secure_password',
        ssl=False,
        service_name='test_client'
    )
    #Client may be a server
    await rpc_client.register_worker('callback', some_func)


    #Send and get some structure, wait for result
    data = await rpc_client.call(
        'test_server',
        'test',
        conf=RabbitMQConfig(
            host='localhost',
            port=5672,
            user='admin',
            password='secure_password',
            ssl=False),
        need_sleep=True
    )
    print(data)

    await asyncio.sleep(5)

    #Just send data, no wait
    data = await rpc_client.send(
        'test_server',
        'test',
        conf=RabbitMQConfig(
            host='localhost',
            port=5672,
            user='admin',
            password='secure_password',
            ssl=False),
        need_sleep=False
    )
    print(data)

    await asyncio.sleep(5)

    #Call a callback
    await rpc_client.send('test_server','callback')

    await asyncio.sleep(5)

    #timeout while running
    try:
        data = await rpc_client.call(
            'test_server',
            'test',
            timeout=5,
            conf=RabbitMQConfig(
                host='localhost',
                port=5672,
                user='admin',
                password='secure_password',
                ssl=False),
            need_sleep=True
        )
        print(f"Got data: {data}")
    except Exception as e:
        print(f"Got exception: {e}")

    await asyncio.sleep(5)

    #Crash while running
    try:
        data = await rpc_client.call(
            'test_server',
            'killer',
        )
        print(f"Got data: {data}")
    except Exception as e:
        print(f"Got exception: {e}")

    await asyncio.sleep(5)

    #Call dead service
    try:
        data = await rpc_client.call(
            'test_server',
            'killer',
        )
        print(f"Got data: {data}")
    except Exception as e:
        print(f"Got exception: {e}")

    await asyncio.sleep(5)

if __name__ == '__main__':
    asyncio.run(main())