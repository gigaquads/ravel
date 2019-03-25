from pybiz.logging import ConsoleLoggerInterface
from pybiz.constants import CONSOLE_LOG_LEVEL


def get_console_logger(name) -> ConsoleLoggerInterface:
    return ConsoleLoggerInterface(name, level=CONSOLE_LOG_LEVEL)
