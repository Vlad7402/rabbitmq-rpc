from typing import Awaitable, TypeVar, Any

from tenacity import retry, stop_after_attempt, RetryError, stop_after_delay

from rabbitmq_rpc.exceptions import RPCError

T = TypeVar('T')

async def with_retry_and_timeout(coro: Awaitable, retry_count: int, timeout: float) -> Any:
    @retry(stop=(stop_after_attempt(retry_count) |
                 stop_after_delay(timeout)))
    async def _with_retry() -> T:
        try:
            return await coro
        except Exception:
            raise
    try:
        return await _with_retry()
    except RetryError as e:
        raise RPCError(f"Operation failed after {retry_count} attempts or {timeout:.1f} sec timeout: {e}")