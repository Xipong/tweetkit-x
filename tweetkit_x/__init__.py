"""X web-session client and read-only research MCP.

The original Python API remains available. Lazy imports let MCP discovery and
pure parsing work without loading an account or the transaction-id dependency.
"""
from importlib import import_module

__all__ = ["TweetKit", "load_cookie", "save_cookie", "parse_cookie",
           "has_auth", "CookieError", "importer"]
__version__ = "0.4.0"


def __getattr__(name):
    if name == "TweetKit":
        value = import_module(".client", __name__).TweetKit
    elif name == "importer":
        value = import_module(".importer", __name__)
    elif name in __all__:
        value = getattr(import_module(".cookie", __name__), name)
    else:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    globals()[name] = value
    return value
