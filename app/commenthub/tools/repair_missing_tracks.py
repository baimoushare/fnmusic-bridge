#!/usr/bin/env python3
"""Repair explicitly requested fnOS tracks/mappings, not comments or a full sync.

Default dry-run DOES read fnOS metadata and search platforms, but never opens a
writable database. --apply adds missing tracks and trusted missing mappings only.
Existing tracks and mappings (including rejected/locked ones) are never replaced.
Run refresh_v2.py separately, scoped to these GUIDs, after reviewing the report.
"""
import argparse
from dataclasses import asdict
import http.client
import json
import math
import os
import re
import socket
import sys
import time
from urllib.parse import urlencode

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
import db as DB
import matcher
import pacing
from adapters import available_sources, get_adapter
from adapters.base import AdapterError
from adapters.http import StdlibTransport
from ingest import IngestBatch
from refresh_v2 import guard_db

ACTOR = "tools/repair_missing_tracks.py"


class RepairError(Exception):
    pass


def normalize_metadata(payload, guid):
    track = (payload.get("data") or {}).get("track") if isinstance(payload, dict) else None
    if not isinstance(track, dict) or track.get("guid") != guid:
        raise RepairError("fnOS metadata missing or GUID mismatch")
    title = track.get("title")
    artists = [a["name"].strip() for a in (track.get("artists") or [])
               if isinstance(a, dict) and isinstance(a.get("name"), str) and a["name"].strip()]
    # fnOS metadata duration is milliseconds (same value as track.duration_ms).
    duration = track.get("duration")
    if (not isinstance(title, str) or not title.strip() or not artists
            or isinstance(duration, bool) or not isinstance(duration, (int, float))
            or not math.isfinite(duration) or duration <= 0):
        raise RepairError("fnOS metadata lacks title, artists, or valid duration")
    album = track.get("album") or {}
    return {"guid": guid, "title": title, "artists": artists,
            "album": album.get("name") if isinstance(album, dict) else None,
            "duration_ms": int(duration)}


class FnMetadata:
    """Read cached credentials only; no login, token refresh, cache writes or logging."""
    def __init__(self, credentials, socket_path, user=None, timeout=15):
        try:
            with open(credentials, encoding="utf-8") as fh:
                cached = json.load(fh)
        except Exception:
            raise RepairError("Cannot read credential cache") from None
        if not isinstance(cached, dict):
            raise RepairError("Credential cache must be a username/token object")
        tokens = [cached.get(user)] if user else list(cached.values())
        self._tokens = list(dict.fromkeys(t for t in tokens if isinstance(t, str) and t))
        if not self._tokens:
            raise RepairError("No cached credentials for selected user")
        self.socket_path, self.timeout = socket_path, timeout

    def __call__(self, guid):
        for token in self._tokens:
            conn = http.client.HTTPConnection("localhost", timeout=self.timeout)
            try:
                conn.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                conn.sock.settimeout(self.timeout)
                conn.sock.connect(self.socket_path)
                conn.request("GET", "/music/api/v1/track/metadata?" + urlencode({"guid": guid}),
                             headers={"Cookie": "music-token=" + token})
                response = conn.getresponse()
                payload = json.loads(response.read())
                if response.status == 200:
                    try:
                        return normalize_metadata(payload, guid)
                    except RepairError:
                        pass
            except Exception:
                # Do not expose HTTP headers, raw response, tokens or exception text.
                pass
            finally:
                conn.close()
        raise RepairError("fnOS metadata unavailable with cached credentials")


def _live_format(text):
    """Only canonicalize trailing '- Live' decoration, not version identity."""
    return re.sub(r"\s*[-–—]\s*live\s*$", " (Live)", str(text or ""), flags=re.I)


def _album_key(text):
    # Pure trailing (Live) is a label, not the concert identity. Keep year,
    # tour, location, disc and all other tokens; do not strip arbitrary brackets.
    return matcher.norm(re.sub(r"\s*[（(]\s*live\s*[）)]\s*$", "", str(text or ""), flags=re.I))


def _specific_version_key(title):
    """Recognize explicit venue/year annotations, never just the word Live.

    Keep every annotation token: distinct venues/years must not be conflated by
    norm_title's bracket removal. No artist aliases or fuzzy equivalence here.
    """
    text = _live_format(title)
    annotations = re.findall(r"[（(【\[]([^）)】\]]+)[）)】\]]", text)
    suffix = re.search(r"\s+[-–—]\s+(.+)$", text)
    if suffix:
        annotations.append(suffix.group(1))
    if not annotations:
        explicit_live = re.search(r"(?<![a-z0-9])(live\s+(?:at|in)\s+.+)$", text, re.I)
        if explicit_live:
            annotations.append(explicit_live.group(1))
    keys = []
    for annotation in annotations:
        tags = matcher.version_tags(annotation)
        if not tags:
            continue
        year = re.search(r"(?<!\d)(?:19|20)\d{2}(?!\d)", annotation)
        venue = re.search(r"\blive\s+(?:at|in)\s+(.+)", annotation, re.I)
        if year or (venue and len(matcher.norm(venue.group(1))) >= 3):
            keys.append(matcher.norm(annotation))
    return tuple(sorted(keys))


def _duration_distance(local, candidate):
    values = (local.get("duration_ms"), candidate.get("duration_ms"))
    if all(isinstance(v, (int, float)) and not isinstance(v, bool)
           and math.isfinite(v) and v > 0 for v in values):
        return abs(values[0] - values[1])
    return None


def evaluate(local, song):
    candidate = asdict(song)
    scored_local = dict(local, title=_live_format(local["title"]))
    scored_candidate = dict(candidate, name=_live_format(candidate["name"]))
    score, reasons, dims = matcher.score_candidate(scored_local, scored_candidate)
    grade = matcher.grade(score, dims)
    guards = []
    if matcher.norm_title(scored_local["title"]) != matcher.norm_title(scored_candidate["name"]):
        guards.append("title_not_exact")
    if matcher.version_tags(local["title"]) != matcher.version_tags(candidate["name"]):
        guards.append("version_not_exact")
    if not matcher.artists_of(local) or matcher.artists_of(local) != matcher.artists_of(candidate):
        guards.append("artists_not_exact")
    distance = _duration_distance(local, candidate)
    album_exact = bool(_album_key(local.get("album")) and
                       _album_key(local.get("album")) == _album_key(candidate.get("album")))
    local_version = _specific_version_key(local["title"])
    candidate_version = _specific_version_key(candidate["name"])
    specific_exact = bool(local_version and local_version == candidate_version)
    # Ordinary recordings do not need album equality. Generic version labels do:
    # a matching venue/year annotation can instead identify the recording, but
    # only with exact title/artists/tags and a tightly bounded duration.
    if matcher.version_tags(local["title"]):
        if (local_version or candidate_version) and local_version != candidate_version:
            guards.append("specific_version_not_exact")
        if not album_exact and not specific_exact:
            guards.append("version_album_not_exact")
        if distance is None or distance > matcher.DUR_CLOSE_MS:
            guards.append("version_duration_not_tight")
    if not song.song_id or grade not in ("high", "medium"):
        guards.append("not_trusted_grade_or_id")
    evidence = {"duration_distance_ms": distance, "album_exact": album_exact,
                "specific_version_exact": specific_exact}
    return {"candidate": candidate, "score": score, "grade": grade, "dims": dims,
            "reasons": reasons, "guards": guards, "accepted": not guards,
            "evidence": evidence}


DURATION_MARGIN_MS = 1000
SCORE_MARGIN = 0.05


def _same_recording(left, right):
    """Only platform identifiers prove duplicates; matching labels alone do not."""
    a, b = left["candidate"], right["candidate"]
    if a["source"] != b["source"]:
        return False
    if a["song_id"] == b["song_id"]:
        return True
    # Distinct IDs with identical metadata are still ambiguous unless the
    # provider supplies the same nonempty recording identifier (song_mid).
    return bool(a.get("song_mid") and a["song_mid"] == b.get("song_mid") and
                all(a.get(k) == b.get(k) for k in
                    ("name", "artists", "album", "duration_ms")))


def select_trusted_candidate(results):
    """Return (chosen, outcome), without search order or popularity tie breakers.

    Within a score tie, prefer album equality only if duration doesn't conflict;
    or prefer a >=1s closer duration if album evidence doesn't conflict. Small
    duration differences and indistinguishable distinct IDs remain unresolved.
    """
    trusted = [r for r in results if r["accepted"]]
    if not trusted:
        return None, "no_candidates" if not results else "no_trusted_candidates"

    def beats(a, b):
        if _same_recording(a, b):
            return True
        score_gap = a["score"] - b["score"]
        if score_gap >= SCORE_MARGIN:
            return True
        if abs(score_gap) > 1e-9:
            return False
        ae, be = a["evidence"], b["evidence"]
        ad, bd = ae["duration_distance_ms"], be["duration_distance_ms"]
        if ae["album_exact"] and not be["album_exact"]:
            return ad is not None and bd is not None and ad < bd + DURATION_MARGIN_MS
        return (ad is not None and bd is not None and ad <= matcher.DUR_CLOSE_MS
                and bd - ad >= DURATION_MARGIN_MS
                and not (be["album_exact"] and not ae["album_exact"]))

    winners = [a for a in trusted if all(beats(a, b) for b in trusted)]
    if not winners or any(not _same_recording(winners[0], b) for b in winners[1:]):
        return None, "tied_top"
    # ID ordering is used only AFTER recording equivalence is proven, to keep
    # the stored mapping stable across provider search-order changes.
    chosen = min(winners, key=lambda r: str(r["candidate"]["song_id"]))
    return chosen, "matched"


class _NestedConnection:
    """IngestBatch's per-method DB.run_tx uses savepoints within our outer T1.

    The real connection is already in BEGIN IMMEDIATE. This facade changes only
    transaction control, leaving schema/upsert SQL to the existing ingest APIs.
    """
    def __init__(self, con):
        self.con = con

    def execute(self, sql, params=()):
        command = sql.strip().upper()
        if command in ("BEGIN", "BEGIN IMMEDIATE"):
            return self.con.execute("SAVEPOINT repair_ingest")
        if command == "COMMIT":
            return self.con.execute("RELEASE repair_ingest")
        if command == "ROLLBACK":
            self.con.execute("ROLLBACK TO repair_ingest")
            return self.con.execute("RELEASE repair_ingest")
        return self.con.execute(sql, params)

    def __getattr__(self, name):
        return getattr(self.con, name)


def apply_plan(path, plans):
    """One all-or-nothing transaction; recheck absence while holding write lock."""
    con = DB.connect(path)
    now = int(time.time())
    try:
        def work(real):
            nested = _NestedConnection(real)
            applied = []
            for plan in plans:
                local = plan["metadata"]
                guid = local["guid"]
                batches = {}
                def batch(source):
                    if source not in batches:
                        batches[source] = IngestBatch(nested, source=source, kind="search", actor=ACTOR, now=now)
                        batches[source].start()
                    return batches[source]
                inserted = real.execute("SELECT 1 FROM tracks WHERE guid=?", (guid,)).fetchone() is None
                if inserted:
                    batch(available_sources()[0]).upsert_track(**local)
                mapped = []
                for source, result in plan["sources"].items():
                    chosen = result.get("chosen")
                    if not chosen or real.execute("SELECT 1 FROM platform_mappings WHERE track_guid=? AND source=?", (guid, source)).fetchone():
                        continue
                    song = chosen["candidate"]
                    batch(source).upsert_mapping(guid, source, song["song_id"], song_mid=song["song_mid"],
                        song_name=song["name"], song_artist="/".join(song["artists"]), song_album=song["album"],
                        duration_ms=song["duration_ms"], score=chosen["score"], grade=chosen["grade"],
                        reasons=matcher.reasons_json(chosen["dims"], None), matcher_version=matcher.MATCHER_VERSION)
                    mapped.append(source)
                for b in batches.values():
                    b.finish(ok=True)
                applied.append({"guid": guid, "track_inserted": inserted, "mappings_inserted": mapped})
            return applied
        return DB.run_tx(con, work)
    finally:
        con.close()


def repair(path, tracks, metadata, adapters, *, apply=False):
    path = guard_db(path)
    plans, stopped = [], set()
    con = DB.connect(path, read_only=True)
    try:
        for guid in dict.fromkeys(tracks):
            if not re.fullmatch(r"[0-9a-fA-F]{32}|[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}", guid):
                raise RepairError("--track requires a full GUID, not a prefix or title")
            local = metadata(guid)
            if local.get("guid") != guid:
                raise RepairError("Metadata provider GUID mismatch")
            plan = {"metadata": local, "track_missing": con.execute("SELECT 1 FROM tracks WHERE guid=?", (guid,)).fetchone() is None, "sources": {}}
            for source, adapter in adapters.items():
                if con.execute("SELECT 1 FROM platform_mappings WHERE track_guid=? AND source=?", (guid, source)).fetchone():
                    plan["sources"][source] = {"status": "existing_mapping_preserved"}
                    continue
                if source in stopped:
                    plan["sources"][source] = {"status": "rate_limited_skipped"}
                    continue
                try:
                    results = [evaluate(local, s) for s in adapter.search(local["title"], local["artists"], limit=20)]
                except AdapterError as exc:
                    if exc.error_kind == "rate_limited":
                        stopped.add(source)
                    plan["sources"][source] = {"status": "search_error", "error_kind": exc.error_kind}
                    continue
                chosen, _ = select_trusted_candidate(results)
                plan["sources"][source] = {"status": "trusted_match" if chosen else "unmapped_no_unambiguous_trusted_match", "chosen": chosen, "candidates": results}
            plans.append(plan)
    finally:
        con.close()
    return {"mode": "apply" if apply else "dry-run", "tracks": plans,
            "applied": apply_plan(path, plans) if apply else [], "rate_limited_sources": sorted(stopped)}


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db", required=True)
    p.add_argument("--track", action="append", required=True)
    p.add_argument("--apply", action="store_true")
    p.add_argument("--credentials", "--credential-file", default="/data/.tokens.json")
    p.add_argument("--socket", default="/run/trim_music.socket")
    p.add_argument("--user")
    p.add_argument("--source", action="append", choices=available_sources())
    p.add_argument("--proxy")
    p.add_argument("--timeout", type=float, default=15)
    args = p.parse_args(argv)
    try:
        guard_db(args.db)
        provider = FnMetadata(args.credentials, args.socket, args.user, args.timeout)
        transport = StdlibTransport()
        adapters = {s: get_adapter(s, transport, proxy=args.proxy, timeout=args.timeout,
                    pacer=pacing.Pacer({"netease": 0.6, "qq": 0.5}[s]))
                    for s in (args.source or available_sources())}
        report = repair(args.db, args.track, provider, adapters, apply=args.apply)
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 3 if report["rate_limited_sources"] else 0
    except Exception as exc:
        # Unexpected credential-bearing exceptions are deliberately not rendered.
        print(json.dumps({"error": str(exc) if isinstance(exc, RepairError) else type(exc).__name__}, ensure_ascii=False), file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
