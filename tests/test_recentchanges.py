import json

from wikipulse.recentchanges import iter_sse, normalize

EVENT = {
    "meta": {"id": "abc", "dt": "2026-11-20T12:00:00Z", "domain": "en.wikipedia.org"},
    "type": "edit",
    "namespace": 0,
    "title": "Helsinki",
    "user": "Example",
    "bot": False,
    "minor": True,
    "wiki": "enwiki",
    "server_name": "en.wikipedia.org",
    "length": {"old": 100, "new": 130},
    "revision": {"old": 1, "new": 2},
    "comment": "long text that is not stored",
}


def test_iter_sse_handles_comments_and_multiline():
    lines = [":ok", "event: message", "id: 1", f"data: {json.dumps(EVENT)}", "", "data: line1", "data: line2", ""]
    out = list(iter_sse(lines))
    assert json.loads(out[0])["title"] == "Helsinki" and out[1] == "line1\nline2"


def test_normalize_flattens_and_drops_comment():
    row = normalize(EVENT)
    assert row["bytes_changed"] == 30 and row["editor"] == "Example" and row["is_minor"]
    assert "comment" not in row


def test_canary_and_incomplete_events_are_skipped():
    assert normalize({**EVENT, "meta": {"domain": "canary"}}) is None
    assert normalize({"meta": {"id": "x"}}) is None


def test_new_page_has_no_old_length():
    row = normalize({**EVENT, "type": "new", "length": {"new": 50}})
    assert row["bytes_changed"] == 50 and row["length_old"] is None
