import time

import pytest

from presolv import errors


def test_hierarchy_and_index():
    for cls in (errors.DnsTimeoutError, errors.NetworkError, errors.ProtocolError,
                errors.ConnectionPoolExhausted, errors.LameServerError):
        assert issubclass(cls, errors.PresolvError)
    e = errors.NetworkError("boom", index=3)
    assert e.index == 3 and "boom" in str(e)
    assert errors.PresolvError("x").index is None


@pytest.mark.parametrize("kind,cls", [
    ("timeout", errors.DnsTimeoutError),
    ("network", errors.NetworkError),
    ("protocol", errors.ProtocolError),
    ("pool_exhausted", errors.ConnectionPoolExhausted),
    ("lame", errors.LameServerError),
])
def test_from_native_maps_kind(kind, cls):
    e = errors.from_native(kind, "m", 9, ["1.1.1.1"], 2.0)
    assert type(e) is cls and e.index == 9


def test_lame_error_has_absolute_retry_at_and_nameservers():
    before = time.time()
    e = errors.from_native("lame", "m", 0, ["1.1.1.1", "8.8.8.8"], 10.0)
    assert e.nameservers == ("1.1.1.1", "8.8.8.8")
    assert before + 9.5 <= e.retry_at <= time.time() + 10.5


def test_unknown_kind_falls_back_to_base():
    assert type(errors.from_native("???", "m", 0, [], 0.0)) is errors.PresolvError
