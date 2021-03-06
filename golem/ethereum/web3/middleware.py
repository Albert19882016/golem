import logging
import socket
from typing import ClassVar

from types import MethodType

from web3.exceptions import CannotHandleRequest

logger = logging.getLogger(__name__)

MAX_ERRORS = 20
RETRIES = 3


class RemoteRPCErrorMiddlewareBuilder:
    """
    Not for use with multiple (incompatible) providers - hence the
    CannotHandleRequest exception in middleware function.
    """

    _cur_errors: ClassVar[int] = 0

    def __init__(self,
                 error_listener: MethodType,
                 max_errors: int = MAX_ERRORS,
                 retries: int = RETRIES) -> None:
        """
        :param error_listener: Function to execute when the maximum number of
        consecutive errors is reached
        :param max_errors: Maximum number of consecutive unrecoverable errors
        """
        self._max_errors = max_errors
        self._retries = retries
        self._err_listener = error_listener

    def build(self, make_request, _web3):
        """ Returns the middleware function """

        def middleware(method, params):
            while True:
                try:
                    result = make_request(method, params)
                except (ConnectionError, ValueError,
                        socket.error, CannotHandleRequest) as exc:
                    logger.warning(
                        'GETH: request failure, retrying: %s',
                        exc,
                    )
                    self._cur_errors += 1
                    if self._cur_errors >= self._max_errors:
                        self.reset()
                        raise
                    if self._cur_errors % self._retries == 0:
                        self._err_listener()
                        return
                else:
                    self.reset()
                    return result

        return middleware

    def reset(self):
        """ Resets the current error number counter """
        self._cur_errors = 0
