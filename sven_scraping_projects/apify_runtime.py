from __future__ import annotations

import threading
from typing import Optional, Any


_lock = threading.Lock()
_actor_loop: Any | None = None


def set_actor_loop(loop: Any | None) -> None:
    global _actor_loop
    with _lock:
        _actor_loop = loop


def get_actor_loop() -> Any | None:
    with _lock:
        return _actor_loop

