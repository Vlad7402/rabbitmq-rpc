class RPCClientException(Exception):
    """Base exception for RPCClient errors."""
    pass

class MQConnectionError(RPCClientException):
    """Raised when there is a connection error."""
    pass

class RPCError(RPCClientException):
    """Raised when there is an RPC-related error."""
    pass

class EventRegistrationError(RPCClientException):
    """Raised when there is an error in event registration."""
    pass
