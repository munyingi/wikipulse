"""Streams Wikimedia's live ``recentchange`` feed into a Fabric Eventstream custom endpoint.

The feed is public Server-Sent Events (https://stream.wikimedia.org/v2/stream/recentchange). Each event is
flattened to the columns of the ``RecentChanges`` table in the Eventhouse and sent in batches to the
Eventstream's custom endpoint, which speaks the Event Hubs protocol.

    export EVENTSTREAM_CONNECTION_STRING="Endpoint=sb://...;EntityPath=..."
    wikipulse-stream --minutes 30
"""

from __future__ import annotations

import argparse
import json
import os
import time
from collections.abc import Iterable, Iterator

STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
USER_AGENT = "wikipulse/1.0 (portfolio project; https://github.com/munyingi/wikipulse)"


def iter_sse(lines: Iterable[str]) -> Iterator[str]:
    """Yield the ``data`` payload of each Server-Sent Event. Multi-line data fields are joined."""
    data: list[str] = []
    for raw in lines:
        line = raw.rstrip("\r\n")
        if line == "":
            if data:
                yield "\n".join(data)
                data = []
            continue
        if line.startswith(":"):
            continue  # comment / keep-alive
        field, _, value = line.partition(":")
        if field == "data":
            data.append(value[1:] if value.startswith(" ") else value)
    if data:
        yield "\n".join(data)


def normalize(event: dict) -> dict | None:
    """Flatten one recentchange event. Returns None for events that should not be stored."""
    meta = event.get("meta") or {}
    if meta.get("domain") == "canary" or "type" not in event:
        return None  # Wikimedia's synthetic health-check events
    length = event.get("length") or {}
    revision = event.get("revision") or {}
    old, new = length.get("old"), length.get("new")
    return {
        "event_id": meta.get("id"),
        "event_time": meta.get("dt"),
        "wiki": event.get("wiki"),
        "server_name": event.get("server_name"),
        "change_type": event.get("type"),
        "namespace": event.get("namespace"),
        "title": event.get("title"),
        "editor": event.get("user"),
        "is_bot": bool(event.get("bot", False)),
        "is_minor": bool(event.get("minor", False)),
        "length_old": old,
        "length_new": new,
        "bytes_changed": (new or 0) - (old or 0) if new is not None or old is not None else None,
        "revision_old": revision.get("old"),
        "revision_new": revision.get("new"),
    }


def events(url: str = STREAM_URL, timeout: float = 60) -> Iterator[dict]:
    import requests

    with requests.get(
        url, stream=True, timeout=timeout, headers={"Accept": "text/event-stream", "User-Agent": USER_AGENT}
    ) as resp:
        resp.raise_for_status()
        for payload in iter_sse(resp.iter_lines(decode_unicode=True)):
            try:
                ev = normalize(json.loads(payload))
            except json.JSONDecodeError:
                continue
            if ev:
                yield ev


class EventstreamSink:
    """Batches events to an Eventstream custom endpoint (Event Hubs compatible)."""

    def __init__(self, connection_string: str, max_batch: int = 500):
        from azure.eventhub import EventHubProducerClient

        self.client = EventHubProducerClient.from_connection_string(connection_string)
        self.max_batch = max_batch
        self.sent = 0

    def send(self, rows: list[dict]) -> None:
        from azure.eventhub import EventData

        batch = self.client.create_batch()
        for row in rows:
            data = EventData(json.dumps(row))
            try:
                batch.add(data)
            except ValueError:  # batch full
                self.client.send_batch(batch)
                batch = self.client.create_batch()
                batch.add(data)
        if len(batch):
            self.client.send_batch(batch)
        self.sent += len(rows)

    def close(self) -> None:
        self.client.close()


def run(minutes: float, connection_string: str, flush_every: int = 200, log=print) -> int:
    sink = EventstreamSink(connection_string)
    deadline = time.monotonic() + minutes * 60
    buffer: list[dict] = []
    try:
        while time.monotonic() < deadline:
            try:
                for ev in events():
                    buffer.append(ev)
                    if len(buffer) >= flush_every:
                        sink.send(buffer)
                        buffer = []
                        log(f"sent {sink.sent} events")
                    if time.monotonic() >= deadline:
                        break
            except Exception as exc:  # the public stream drops idle connections; reconnect
                log(f"stream interrupted ({exc}); reconnecting")
                time.sleep(2)
        if buffer:
            sink.send(buffer)
    finally:
        sink.close()
    return sink.sent


def main() -> None:
    ap = argparse.ArgumentParser(description="Send Wikimedia recent changes to a Fabric Eventstream")
    ap.add_argument("--minutes", type=float, default=30)
    args = ap.parse_args()
    conn = os.environ.get("EVENTSTREAM_CONNECTION_STRING")
    if not conn:
        raise SystemExit("Set EVENTSTREAM_CONNECTION_STRING (Eventstream > custom endpoint > Event Hub keys)")
    print(f"sent {run(args.minutes, conn)} events")


if __name__ == "__main__":
    main()
