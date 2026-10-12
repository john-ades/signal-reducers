"""Signal Reducers P0. Models propose; the runtime validates and owns state."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("signal-reducers")
except PackageNotFoundError:
    __version__ = "0+unknown"
