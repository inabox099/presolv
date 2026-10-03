from typing import Optional

RawResult = tuple[int, Optional[bytes], Optional[str], str, str, str, float, list[str], float]

def native_version() -> str: ...
def system_nameservers() -> list[tuple[str, int]]: ...

class Session:
    def submit(
        self, index: int, wire: bytes, nameserver: Optional[tuple[str, int]], protocol: str
    ) -> None: ...
    def next(self, timeout: float) -> Optional[RawResult]: ...
    def close(self) -> None: ...

class NativeEngine:
    def __init__(
        self,
        nameservers: list[tuple[str, int]],
        timeout: float,
        retries: int,
        rate_limit: Optional[float],
        burst: Optional[int],
        pool_size: int,
        idle_timeout: float,
        blacklist_after: Optional[int],
        blacklist_duration: float,
        worker_threads: Optional[int],
    ) -> None: ...
    def session(self) -> Session: ...
    def close(self) -> None: ...
