from __future__ import annotations

import ipaddress
import threading
from typing import Callable, Iterable, Iterator, Literal, Optional, Sequence, Tuple, Union

import dns.message

from . import _presolv
from . import errors
from ._convert import convert_response
from .errors import PresolvError
from .query import Query, coerce_query
from .result import Result

NameServer = Union[str, Tuple[str, int]]


def _normalize_nameservers(items: Sequence[NameServer]) -> list:
    out = []
    for item in items:
        ip, port = (item, 53) if isinstance(item, str) else (item[0], int(item[1]))
        out.append((str(ipaddress.ip_address(ip)), port))
    return out


class _ResultIterator:
    """Iterator returned by `Resolver.resolve`. A feeder thread drains the source;
    `__next__` only receives results (spec §5.3)."""

    def __init__(self, resolver: "Resolver", source: Iterable) -> None:
        self._r = resolver
        self._session = resolver._native.session()
        self._sem = threading.BoundedSemaphore(resolver._concurrency)
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._submitted = 0
        self._consumed = 0
        self._feeder_done = False
        self._source_exc: Optional[BaseException] = None
        self._finished = False
        self._thread = threading.Thread(
            target=self._feed, args=(source,), daemon=True, name="presolv-feeder"
        )
        self._thread.start()

    # -- feeder ---------------------------------------------------------------
    def _acquire_permit(self) -> bool:
        while not self._sem.acquire(timeout=0.1):
            if self._stop.is_set():
                return False
        if self._stop.is_set():
            self._sem.release()
            return False
        return True

    def _feed(self, source: Iterable) -> None:
        index = 0
        try:
            it = iter(source)
            while True:
                if not self._acquire_permit():  # permit BEFORE pulling: don't over-read a broker
                    return
                submitted = False
                try:
                    try:
                        item = next(it)
                    except StopIteration:
                        return
                    query = coerce_query(item)
                    wire = query.message.to_wire()
                    ns = None if query.nameserver is None else (query.nameserver, query.port or 53)
                    self._session.submit(index, wire, ns, query.protocol)
                    submitted = True
                finally:
                    if not submitted:
                        self._sem.release()
                with self._lock:
                    self._submitted += 1
                index += 1
        except BaseException as exc:  # noqa: BLE001 - re-raised from __next__
            self._source_exc = exc
        finally:
            with self._lock:
                self._feeder_done = True

    # -- iterator -------------------------------------------------------------
    def __iter__(self) -> "_ResultIterator":
        return self

    def __next__(self) -> Result:
        while True:
            if self._finished:
                raise StopIteration
            if self._r._closed:
                self.close()
                raise StopIteration
            with self._lock:
                done = self._feeder_done and self._consumed >= self._submitted
            if done:
                exc, self._source_exc = self._source_exc, None
                self.close()
                if exc is not None:
                    raise exc
                raise StopIteration
            raw = self._session.next(0.1)
            if raw is None:
                continue
            with self._lock:
                self._consumed += 1
            self._sem.release()
            result = self._r._build_result(raw)
            if result.error is not None and self._r._raise_on_error:
                raise result.error
            return result

    def close(self) -> None:
        if self._finished:
            return
        self._finished = True
        self._stop.set()
        self._session.close()
        if threading.current_thread() is not self._thread:
            self._thread.join(timeout=1.0)  # daemon: a source blocked forever can't hang exit

    def __del__(self) -> None:  # safety net
        try:
            self.close()
        except Exception:
            pass


class Resolver:
    def __init__(
        self,
        nameservers: Optional[Sequence[NameServer]] = None,
        timeout: float = 5.0,
        retries: int = 2,
        rate_limit: Optional[float] = None,
        burst: Optional[int] = None,
        pool_size: int = 100,
        idle_timeout: float = 60.0,
        concurrency: int = 1000,
        response_format: Literal["message", "dict"] = "message",
        raise_on_error: bool = False,
        blacklist_after: Optional[int] = 5,
        blacklist_duration: float = 30.0,
        worker_threads: Optional[int] = None,
    ) -> None:
        if response_format not in ("message", "dict"):
            raise ValueError(f"invalid response_format: {response_format!r}")
        if concurrency < 1:
            raise ValueError("concurrency must be >= 1")
        ns = (
            _normalize_nameservers(nameservers)
            if nameservers is not None
            else list(_presolv.system_nameservers())  # read once, at construction (spec §9)
        )
        self._concurrency = concurrency
        self._response_format = response_format
        self._raise_on_error = raise_on_error
        self._closed = False
        self._native = _presolv.NativeEngine(
            ns, timeout, retries, rate_limit, burst, pool_size, idle_timeout,
            blacklist_after or None, blacklist_duration, worker_threads,
        )

    # -- public API -----------------------------------------------------------
    def resolve(self, queries: Iterable[Union[Query, dns.message.Message]]) -> Iterator[Result]:
        if self._closed:
            raise RuntimeError("Resolver is closed")
        return _ResultIterator(self, queries)

    def resolve_stream(
        self,
        source: Iterable[Union[Query, dns.message.Message]],
        on_result: Callable[[Result], None],
        on_error: Optional[Callable[[Exception], None]] = None,
    ) -> None:
        it = self.resolve(source)
        try:
            while True:
                try:
                    result = next(it)
                except StopIteration:
                    return
                except PresolvError:
                    raise  # raise_on_error=True: per-query failure propagates (spec §5.4)
                except Exception as exc:  # source / engine failure
                    if on_error is None:
                        raise
                    on_error(exc)
                    return
                on_result(result)  # exceptions from the handler propagate
        finally:
            it.close()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._native.close()

    def __enter__(self) -> "Resolver":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass

    # -- internals ------------------------------------------------------------
    def _build_result(self, raw) -> Result:
        index, wire, kind, message, ns, proto, rtt_ms, lame_ns, lame_in = raw
        response = error = None
        if wire is not None:
            try:
                response = convert_response(wire, self._response_format)
            except errors.ProtocolError as exc:
                exc.index = index
                error = exc
        else:
            error = errors.from_native(kind, message, index, lame_ns, lame_in)
        return Result(response, error, ns, proto, rtt_ms, index)
