import socket
import struct
import threading
import time

import dns.flags
import dns.message
import dns.rcode
import dns.rdatatype
import dns.rrset
import pytest


class MockDns:
    """UDP+TCP mock DNS server on one ephemeral 127.0.0.1 port.

    Behaviour is chosen by the first label of the query name:
      drop.*     -> never answer          short.*  -> 5-byte garbage with matching id
      slow.*     -> answer after 0.3 s    nxdomain.* -> NXDOMAIN
      tc.*       -> set TC flag           anything else -> A 192.0.2.1 (A queries)
    `drop_all=True` ignores labels and never answers.
    """

    def __init__(self, drop_all=False):
        self.drop_all = drop_all
        self.received = []  # (qname, "udp"|"tcp")
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._threads = []
        for _ in range(50):
            udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            udp.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4 * 1024 * 1024)
            udp.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4 * 1024 * 1024)
            udp.bind(("127.0.0.1", 0))
            port = udp.getsockname()[1]
            tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            tcp.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                tcp.bind(("127.0.0.1", port))
            except OSError:
                udp.close()
                tcp.close()
                continue
            break
        else:  # pragma: no cover
            raise RuntimeError("cannot bind udp+tcp on same port")
        self.udp, self.tcp, self.port = udp, tcp, port
        self.udp.settimeout(0.1)
        self.tcp.listen(64)
        self.tcp.settimeout(0.1)
        for target in (self._udp_loop, self._tcp_loop):
            t = threading.Thread(target=target, daemon=True)
            t.start()
            self._threads.append(t)

    # ---- helpers for tests -------------------------------------------------
    @property
    def ns(self):
        """Value usable in Resolver(nameservers=[...])."""
        return ("127.0.0.1", self.port)

    def count(self, label=None, proto=None):
        with self._lock:
            return sum(
                1
                for q, p in self.received
                if (label is None or q.startswith(label + "."))
                and (proto is None or p == proto)
            )

    def close(self):
        self._stop.set()
        for s in (self.udp, self.tcp):
            try:
                s.close()
            except OSError:
                pass

    # ---- server ------------------------------------------------------------
    def _answer(self, data, proto):
        q = dns.message.from_wire(data)
        qname = q.question[0].name.to_text()
        with self._lock:
            self.received.append((qname, proto))
        if self.drop_all:
            return None
        label = qname.split(".")[0]
        if label == "drop":
            return None
        if label == "short":
            return data[:2] + b"\x80\x00\x00"
        r = dns.message.make_response(q)
        if label == "nxdomain":
            r.set_rcode(dns.rcode.NXDOMAIN)
        elif label == "tc":
            r.flags |= dns.flags.TC
        elif q.question[0].rdtype == dns.rdatatype.A:
            r.answer.append(
                dns.rrset.from_text(q.question[0].name, 60, "IN", "A", "192.0.2.1")
            )
        if label == "slow":
            time.sleep(0.3)
        return r.to_wire()

    def _udp_reply(self, data, addr):
        try:
            out = self._answer(data, "udp")
            if out is not None:
                self.udp.sendto(out, addr)
        except Exception:
            pass

    def _udp_loop(self):
        while not self._stop.is_set():
            try:
                data, addr = self.udp.recvfrom(65535)
            except socket.timeout:
                continue
            except OSError:
                return
            threading.Thread(target=self._udp_reply, args=(data, addr), daemon=True).start()

    def _tcp_conn(self, conn):
        conn.settimeout(0.5)
        send_lock = threading.Lock()

        def reply(data):
            try:
                out = self._answer(data, "tcp")
                if out is not None:
                    with send_lock:
                        conn.sendall(struct.pack("!H", len(out)) + out)
            except Exception:
                pass

        try:
            while not self._stop.is_set():
                try:
                    hdr = self._recv_exact(conn, 2)
                except socket.timeout:
                    continue
                if hdr is None:
                    return
                (n,) = struct.unpack("!H", hdr)
                data = self._recv_exact(conn, n)
                if data is None:
                    return
                threading.Thread(target=reply, args=(data,), daemon=True).start()
        except OSError:
            return
        finally:
            conn.close()

    @staticmethod
    def _recv_exact(conn, n):
        buf = b""
        while len(buf) < n:
            chunk = conn.recv(n - len(buf))
            if not chunk:
                return None
            buf += chunk
        return buf

    def _tcp_loop(self):
        while not self._stop.is_set():
            try:
                conn, _ = self.tcp.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            threading.Thread(target=self._tcp_conn, args=(conn,), daemon=True).start()


@pytest.fixture
def mock_dns():
    servers = []

    def make(**kw):
        s = MockDns(**kw)
        servers.append(s)
        return s

    yield make
    for s in servers:
        s.close()


def closed_tcp_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port
