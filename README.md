# 🐇📡 RabbitMQ RPC Client

<p align="center">
    <img src="https://img.shields.io/badge/RabbitMQ-FF6600.svg?style=for-the-badge&logo=RabbitMQ&logoColor=white" alt="RabbitMQ">
    <img src="https://img.shields.io/badge/PyPI-3775A9.svg?style=for-the-badge&logo=PyPI&logoColor=white" alt="PyPI">
    <img src="https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54" alt="Python">
    <img src="https://img.shields.io/badge/license-Apache%202.0-blue.svg?style=for-the-badge" alt="License">
</p>

`rabbitmq_rpc` is a powerful Python package that simplifies the implementation of RPC (Remote Procedure Call) patterns in event-driven microservices. Built on top of the `aio-pika` library, it abstracts the complexities of asynchronous communication with RabbitMQ, providing a seamless and efficient experience for developers.
Forked from <a href="https://github.com/deepmancer/rabbitmq-rpc" target="_blank">deepmancer</a>

---


| **Source Code** | **Website**                                                                |
|:-----------------|:---------------------------------------------------------------------------|
| <a href="https://github.com/Vlad7402/rabbitmq-rpc" target="_blank">github.com/Vlad7402/rabbitmq-rpc</a> | <a href="https://github.com/Vlad7402/rabbitmq-rpc" target="_blank">github.com/Vlad7402/rabbitmq-rpc</a> |

---


## ✨ Features

- **🚀 Asynchronous RPC Client:** Fully built on `aio-pika`, enabling non-blocking inter-service communication.
- **🌐 Distributed Environment Ready:** Effortlessly connects services across containers and different URLs.
- **📜 Event Registration & Handling:** Easily define, register, and handle events with custom event handlers.
- **🔒 Thread-Safe Connection:** Utilizes a singleton design pattern to maintain a single instance of the RPC client across threads.
- **⏱️ Retry & Timeout Mechanism:** Built-in support for retrying failed calls and handling timeouts with `with_retry_and_timeout`.
- **🛠️ No Server-side Implementation Required:** Just a running RabbitMQ server—no need for additional RPC server implementations.

## 📦 Installation

Get started by installing `rabbitmq_rpc` using pip:

```sh
pip install git+https://github.com/Vlad7402/rabbitmq-rpc.git
```

## 🛠️ Quick Start
See examples inside:
```sh
rabbtmq-rpc/rabbtmq_rpc/Example
```
## 🛡️ Error Handling

`rabbitmq_rpc` provides custom exceptions to handle various connection and RPC-related issues:

- `MQConnectionError`
- `RPCError`
- `EventRegistrationError`
- `RPCClientException`

## 🔌 Disconnecting

Gracefully disconnect from RabbitMQ when you're done:

```python
await rpc_client.close()
```

## 📄 License

This project is licensed under the Apache License 2.0. For more details, see the [LICENSE](https://github.com/deepmancer/rabbitmq-rpc/blob/main/LICENSE) file.

---

**Elevate your microservices communication with `rabbitmq_rpc` today!** 🐇📡
