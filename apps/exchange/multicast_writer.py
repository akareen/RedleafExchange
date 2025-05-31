# apps/exchange/multicast_writer.py
import json, socket
from apps.exchange.settings import get_settings
SET = get_settings()

class MulticastWriter:
    def __init__(self):
        self.addr = (SET.mcast_group, SET.mcast_port)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self.sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 1)

    def _send(self, payload):
        self.sock.sendto(json.dumps(payload, separators=(",", ":")).encode(), self.addr)

    # public API expected by Exchange
    def record_order(self, o):  self._send({"type": "ORDER",  **o.__dict__})
    def record_trade(self, t):  self._send({"type": "TRADE",  **t.__dict__})
    def record_cancel(self, i, oid): self._send({"type": "CANCEL", "instrument_id": i, "order_id": oid})
    # rebuild helpers (not used)
    def list_instruments(self):  # for cold rebuild
        return []

    def iter_orders(self, instr):
        return []  # yields nothing

    def create_instrument(self, instr):  # called on POST /new_book
        pass