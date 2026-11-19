# -*- coding: utf-8 -*-
"""Cross-platform OS shutdown & interrupt signal guard utilities."""

from contextlib import contextmanager
import ctypes
import signal
import sys
import threading
from typing import Any, Callable, Dict, Iterator, Optional

_WIN_CTRL_NAMES: Dict[int, str] = {
    0: "CTRL_C_EVENT",
    1: "CTRL_BREAK_EVENT",
    2: "CTRL_CLOSE_EVENT",
    5: "CTRL_LOGOFF_EVENT",
    6: "CTRL_SHUTDOWN_EVENT",
}


class DownloadInterruptedError(Exception):
    """Raised when a historical download is interrupted and partial data is saved."""


def _safe_signal_name(signum: int) -> str:
    """Convert numeric signal code to a readable name."""
    try:
        return signal.Signals(signum).name
    except (ValueError, AttributeError):
        return str(signum)


@contextmanager
def graceful_shutdown_guard(
    on_shutdown: Optional[Callable[[str], None]] = None,
    stop_event: Optional[threading.Event] = None,
) -> Iterator[threading.Event]:
    """
    Context manager trapping OS interrupt/shutdown signals on macOS, Linux,
    and Windows without executing unsafe I/O inside signal handlers.

    On first signal, sets ``stop_event`` and invokes ``on_shutdown(sig_name)``
    if provided so long-running downloads can flush partial data cleanly.
    """
    event = stop_event if stop_event is not None else threading.Event()
    is_main_thread = threading.current_thread() is threading.main_thread()
    previous_handlers: Dict[int, Any] = {}
    win_handler_ref: Any = None
    kernel32: Any = None

    def _trigger(sig_name: str) -> None:
        first_trigger = not event.is_set()
        event.set()
        if first_trigger and on_shutdown is not None:
            on_shutdown(sig_name)

    if is_main_thread:
        def _posix_handler(signum: int, _frame: Any) -> None:
            _trigger(_safe_signal_name(signum))

        candidate_signals = [signal.SIGINT, signal.SIGTERM]
        for optional_name in ("SIGHUP", "SIGBREAK"):
            sig = getattr(signal, optional_name, None)
            if sig is not None:
                candidate_signals.append(sig)

        for sig in candidate_signals:
            try:
                previous_handlers[sig] = signal.getsignal(sig)
                signal.signal(sig, _posix_handler)
            except (OSError, RuntimeError, ValueError, AttributeError):
                pass

    if sys.platform == "win32":
        win_factory = getattr(ctypes, "WINFUNCTYPE", None)
        windll = getattr(ctypes, "windll", None)
        if win_factory is not None and windll is not None:
            kernel32 = getattr(windll, "kernel32", None)
            if kernel32 is not None:
                ctrl_proto = win_factory(ctypes.c_bool, ctypes.c_uint)

                @ctrl_proto
                def _win_ctrl_handler(ctrl_type: int) -> bool:
                    if ctrl_type in _WIN_CTRL_NAMES:
                        _trigger(_WIN_CTRL_NAMES[ctrl_type])
                        return True
                    return False

                win_handler_ref = _win_ctrl_handler
                kernel32.SetConsoleCtrlHandler(win_handler_ref, True)

    try:
        yield event
    finally:
        for sig, prev in previous_handlers.items():
            try:
                signal.signal(sig, prev)
            except (OSError, RuntimeError, ValueError, AttributeError):
                pass
        if kernel32 is not None and win_handler_ref is not None:
            try:
                kernel32.SetConsoleCtrlHandler(win_handler_ref, False)
            except Exception:
                pass
