"""Wrap provider bytes without parsing and reserializing their JSON payload."""
import json
import time
from pathlib import Path
BOOT_ID = Path('/proc/sys/kernel/random/boot_id').read_text().strip()


def frame(data=None, *, raw=None, recv_ms=None, recv_mono_ns=None, capture=None):
    if recv_ms is None: recv_ms = time.time()*1000
    if recv_mono_ns is None: recv_mono_ns = time.monotonic_ns()
    if raw is None:
        raw = json.dumps(data, separators=(',', ':'), ensure_ascii=False).encode()
    elif isinstance(raw, str):
        raw = raw.encode()
    # JSONL must remain one physical line, even for an upstream pretty-printed frame.
    if b'\n' in raw or b'\r' in raw:
        raw = json.dumps(json.loads(raw), separators=(',', ':'), ensure_ascii=False).encode()
    metadata=b',"_capture":'+json.dumps(capture,separators=(',',':')).encode() if capture else b''
    return (b'{"recv_ms":'+str(round(recv_ms,3)).encode()+b',"recv_mono_ns":'+str(recv_mono_ns).encode()
            +metadata+b',"data":'+raw+b'}\n')
