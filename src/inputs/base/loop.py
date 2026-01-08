import asyncio
import typing as T

from inputs.base import Sensor, SensorConfig

R = T.TypeVar("R")
ConfigType = T.TypeVar("ConfigType", bound="SensorConfig")


class FuserInput(Sensor[ConfigType, R]):
    """
    Input polling implementation using a continuous asynchronous loop.

    Improvements:
    - Supports graceful shutdown
    - Handles polling errors without crashing the loop
    - Avoids busy-loop behavior when no input is available
    """

    def __init__(self, config: ConfigType):
        """
        Initialize the FuserInput.

        Parameters
        ----------
        config : ConfigType
            Sensor configuration
        """
        super().__init__(config)
        self._stop_event = asyncio.Event()

    def stop(self) -> None:
        """
        Signal the listen loop to stop gracefully.
        """
        self._stop_event.set()

    async def _listen_loop(self) -> T.AsyncIterator[R]:
        """
        Main asynchronous polling loop.

        Continuously polls for input events and yields them as they
        become available. The loop can be stopped gracefully and is
        resilient to transient polling errors.
        """
        while not self._stop_event.is_set():
            try:
                result = await self._poll()

                # Convention: None means "no input available"
                if result is None:
                    # Small sleep to avoid a tight busy loop
                    await asyncio.sleep(0.01)
                    continue

                yield result

            except asyncio.CancelledError:
                # Allow the task to be cancelled cleanly
                break

            except Exception as e:
                # Prevent unexpected poll errors from crashing the loop
                self.logger.exception(
                    "Unhandled exception in FuserInput polling loop: %s", e
                )
                # Backoff to avoid rapid failure loops
                await asyncio.sleep(0.5)

    async def _poll(self) -> T.Optional[R]:
        """
        Poll for the next input event.

        Returns
        -------
        Optional[R]
            The next raw input event, or None if no input is available.

        Raises
        ------
        NotImplementedError
            Must be implemented by subclasses.
        """
        raise NotImplementedError
