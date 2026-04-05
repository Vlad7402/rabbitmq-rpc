import logging
from abc import ABC, abstractmethod

_log_handler = logging.StreamHandler()
_log_handler.setLevel(logging.DEBUG)
_log_formatter = logging.Formatter('%(name)s: %(asctime)s - %(levelname)s:: %(message)s')
_log_handler.setFormatter(_log_formatter)

def get_log_handler() -> logging.Handler:
    return _log_handler

class AsyncMixin(ABC):
    def __init__(self, *args, **kwargs):
        """
        Standard constructor used for arguments pass
        Do not override. Use __ainit__ instead
        """
        self.__storedargs = args, kwargs
        self.async_initialized = False

    @abstractmethod
    async def __ainit__(self, *args, **kwargs):
        """Async constructor, you should implement this"""

    async def __initobj(self):
        """Crutch used for __await__ after spawning"""
        assert not self.async_initialized
        self.async_initialized = True
        # pass the parameters to __ainit__ that passed to __init__
        await self.__ainit__(*self.__storedargs[0], **self.__storedargs[1])
        return self

    def __await__(self):
        return self.__initobj().__await__()