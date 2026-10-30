"""Read-only live-library comment coverage, independent of the production webapp.

``build_coverage(fn_db, comment_db, alias_db=None)`` accepts filesystem paths.
Only the optional, separate alias SQLite file is written. No network calls,
fetch jobs, source migrations, or source database writes are performed.
Coverage means stored comments linked by conservative metadata evidence; platform
mapping confidence is diagnostic, not a retroactive filter on stored comments.
"""

import hashlib
import json
from pathlib import Path
import sqlite3
import re
import unicodedata
from contextlib import closing


_BUCKETS = ("0", "1–20", "21–100", "101–500", "500+", "未匹配")


def _clean(value):
    return " ".join(unicodedata.normalize("NFKC", str(value or "")).casefold().split())


def _artists(value):
    """Accept only a JSON list of names or structured {name: ...} artists."""
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (ValueError, TypeError):
            return ()
    if not isinstance(value, list):
        return ()
    names = [x.get("name") if isinstance(x, dict) else x for x in value]
    return tuple(sorted({_clean(x) for x in names if isinstance(x, str) and _clean(x)}))


def _duration(value):
    try:
        return max(0, int(value or 0))
    except (ValueError, TypeError, OverflowError):
        return 0


def _key(row):
    return _clean(row["title"]), _artists(row["artists"])


def _complete(row):
    title, artists = _key(row)
    return bool(title and artists and row["duration_ms"] > 0)


def _versioned(title):
    # Same explicit vocabulary/boundaries as the repair matcher's nonneutral tags.
    english = ("live", "acoustic", "unplugged", "remix", "remixed", "dj", "mix",
               "instrumental", "off vocal", "offvocal", "karaoke", "piano version",
               "guitar version", "inst", "remaster", "remastered", "cover")
    chinese = ("现场", "演唱会", "live版", "不插电", "混音", "电音", "加快", "加速", "慢摇",
               "纯音乐", "纯伴奏", "钢琴版", "纯钢琴", "钢琴曲", "吉他版", "吉他曲",
               "演奏版", "演奏曲", "器乐版", "古筝版", "小提琴版", "口琴版", "电子琴版",
               "八音盒版", "音乐盒版", "重制", "母带", "翻唱", "重唱", "女生版", "男生版",
               "温柔男声版", "深情版", "新版", "重置")
    title = _clean(title)
    return (any(word in title for word in chinese) or re.search(r"(?<!无)伴奏", title)
            or any(re.search(r"(?<![a-z0-9])" + re.escape(word) + r"(?![a-z0-9])", title)
                   for word in english))


def _consistent(old, current):
    # Album differences alone often mean compilations, not different recordings.
    # Explicit live/remaster/etc titles need additional nonempty exact album evidence.
    album_ok = (not _versioned(old["title"]) or
                bool(_clean(old.get("album"))) and
                _clean(old.get("album")) == _clean(current.get("album")))
    return (_complete(old) and _complete(current) and _key(old) == _key(current)
            and abs(old["duration_ms"] - current["duration_ms"]) <= 1000 and album_ok)


def _fingerprint(row):
    # Duration is exact here: aliases must not survive gradual metadata drift.
    payload = [_key(row), row["duration_ms"], _clean(row.get("album")),
               row.get("path") or "", row.get("audio_fingerprint") or ""]
    return hashlib.sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True).encode()).hexdigest()


def _readonly(path):
    con = sqlite3.connect(Path(path).resolve().as_uri() + "?mode=ro", uri=True, timeout=3)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA query_only=ON")
    con.execute("BEGIN")
    return con


def _read_sources(fn_db, comment_db):
    with closing(_readonly(fn_db)) as fn, closing(_readonly(comment_db)) as ch:
        current = {}
        for r in fn.execute("""SELECT t.id,t.guid,t.title,t.duration_ms,al.name AS album,
                af.path,af.fingerprint AS audio_fingerprint
                FROM track t JOIN audio_file af ON af.id=t.audio_file_id
                LEFT JOIN album al ON al.id=t.album_id
                WHERE af.is_physical_file_deleted=0 AND t.is_audio_file_deleted=0
                  AND t.is_admin_deleted=0 ORDER BY t.guid"""):
            row = dict(r)
            row["artists"] = []
            row["duration_ms"] = _duration(row["duration_ms"])
            current[row["guid"]] = row
        by_id = {r["id"]: r for r in current.values()}
        for r in fn.execute("""SELECT ta.track_id, ar.guid, ar.name FROM track_artist ta
                JOIN artist ar ON ar.id=ta.artist_id
                ORDER BY ta.track_id, ta.artist_order, ar.id"""):
            if r["track_id"] in by_id:
                by_id[r["track_id"]]["artists"].append({"guid": r["guid"], "name": r["name"]})
        old = {}
        for r in ch.execute("""SELECT guid,title,artists_json,album,duration_ms,path FROM tracks
                               WHERE active=1 AND deleted_at IS NULL ORDER BY guid"""):
            row = dict(r)
            row["artists"] = row.pop("artists_json")
            row["duration_ms"] = _duration(row["duration_ms"])
            old[row["guid"]] = row
        comments = [dict(r) for r in ch.execute(
            "SELECT source,track_guid,COUNT(*) AS n FROM comments GROUP BY source,track_guid")]
        mappings = [dict(r) for r in ch.execute(
            "SELECT track_guid,source,grade,state,locked FROM platform_mappings")]
        sync = [dict(r) for r in ch.execute(
            "SELECT track_guid,source,status,platform_total FROM track_sync_state")]
        known = {r["source"]: int(r["n"] or 0) for r in ch.execute("""
            SELECT s.source,SUM(COALESCE(s.platform_total,0)) AS n
            FROM track_sync_state s JOIN platform_mappings m
              ON m.track_guid=s.track_guid AND m.source=s.source
            WHERE m.grade IN ('high','medium') AND m.state='active'
              AND s.platform_total IS NOT NULL GROUP BY s.source""")}
    return current, old, comments, mappings, sync, known


def alias_diff(saved_rows, desired):
    """算别名表要改哪些行：返回 (要删的 snapshot_guid, 要写的整行)。

    ``desired`` 是 {snapshot_guid: (current_guid, old_fp, current_fp)}。
    以前每次统计都把整表 DELETE + 重插（生产上 7,700 行，光提交就 ~9.5s、
    还要 fsync 一轮），而绝大多数时候一行都没变。
    """
    have = {row[0]: (row[1], row[2], row[3]) for row in saved_rows}
    drop = [key for key, value in have.items() if desired.get(key) != value]
    write = [(key, *value) for key, value in desired.items() if have.get(key) != value]
    return drop, write


def _resolve(current, old, alias_db):
    by_key = {}
    for row in current.values():
        if _complete(row):
            by_key.setdefault(_key(row), []).append(row)
    mapped, ambiguous, reused, invalidated = {}, 0, 0, 0
    aliases = None
    fingerprints = {}

    def fingerprint(kind, row):
        key = (kind, row.get("guid"))
        if key not in fingerprints:
            fingerprints[key] = _fingerprint(row)
        return fingerprints[key]

    try:
        if alias_db is not None:
            aliases = sqlite3.connect(str(alias_db), timeout=3)
            aliases.execute("""CREATE TABLE IF NOT EXISTS comment_guid_aliases (
                snapshot_guid TEXT PRIMARY KEY, current_guid TEXT NOT NULL,
                snapshot_fingerprint TEXT NOT NULL, current_fingerprint TEXT NOT NULL)""")
            saved = aliases.execute("SELECT * FROM comment_guid_aliases").fetchall()
            for old_guid, current_guid, old_fp, current_fp in saved:
                a, b = old.get(old_guid), current.get(current_guid)
                if (a and b and _consistent(a, b) and fingerprint("snapshot", a) == old_fp
                        and fingerprint("current", b) == current_fp
                        and (old_guid not in current or old_guid == current_guid)):
                    mapped[old_guid] = current_guid
                    reused += 1
                else:
                    invalidated += 1
            # Replace atomically below, removing aliases for deleted/reused endpoints.
        for guid, row in old.items():
            if guid in mapped or not _complete(row):
                continue
            if guid in current:
                if _consistent(row, current[guid]):
                    mapped[guid] = guid
                # A GUID collision without evidence must not fall back to another song.
                continue
            hits = [r for r in by_key.get(_key(row), []) if _consistent(row, r)]
            if len(hits) == 1:
                mapped[guid] = hits[0]["guid"]
            elif len(hits) > 1:
                ambiguous += 1
        if aliases is not None:
            desired = {a: (b, fingerprint("snapshot", old[a]), fingerprint("current", current[b]))
                       for a, b in mapped.items()}
            drop, write = alias_diff(saved, desired)
            if drop or write:
                with aliases:
                    if drop:
                        aliases.executemany(
                            "DELETE FROM comment_guid_aliases WHERE snapshot_guid=?",
                            [(key,) for key in drop])
                    if write:
                        aliases.executemany(
                            "INSERT OR REPLACE INTO comment_guid_aliases VALUES (?,?,?,?)", write)
    finally:
        if aliases is not None:
            aliases.close()
    return mapped, ambiguous, reused, invalidated


def build_coverage(fn_db, comment_db, alias_db=None):
    """Return webapp-compatible coverage data plus explicit diagnostic counts.

    Sources must use the live FnOS and CommentHub v2 schemas. ``alias_db`` may
    be ``HOME / 'comment-guid-aliases.db'``; its parent directory must exist.
    It must not resolve to either source DB. Omit it for stateless calculation.
    Errors produce ``{'available': False, 'reason': ...}``, never empty success.
    ``coverage_diagnostics.uncovered_reason_counts`` overlap; the separately
    labelled ``uncovered_partition`` is exclusive and sums to uncovered_tracks.
    """
    try:
        if alias_db is not None:
            alias_path = Path(alias_db).resolve()
            for source in (fn_db, comment_db):
                source_path = Path(source).resolve()
                if alias_path == source_path or (alias_path.exists() and source_path.exists()
                                                 and alias_path.samefile(source_path)):
                    raise ValueError("alias_db must be separate from both source databases")
        if not Path(fn_db).is_file() or not Path(comment_db).is_file():
            return {"available": False, "reason": "飞牛曲库或评论库数据库不可用"}
        current, old, comments, mappings, sync, known = _read_sources(fn_db, comment_db)
        mapped, ambiguous, reused, invalidated = _resolve(current, old, alias_db)
        aliases = {}
        for a, b in mapped.items():
            aliases.setdefault(b, []).append(a)
        counts, source_counts, covered_by_source = {}, {}, {}
        for r in comments:
            source_counts[r["source"]] = source_counts.get(r["source"], 0) + r["n"]
            guid = mapped.get(r["track_guid"])
            if guid is not None:
                counts[guid] = counts.get(guid, 0) + r["n"]
                covered_by_source.setdefault(r["source"], set()).add(guid)
        covered = set(counts)
        platform = {}
        for r in mappings:
            if r["track_guid"] in mapped:
                platform.setdefault(mapped[r["track_guid"]], []).append(r)
        sync_by_current = {}
        for r in sync:
            if r["track_guid"] in mapped:
                sync_by_current.setdefault(mapped[r["track_guid"]], []).append(r)
        def evidence(pm, states):
            return {
                "no_platform_mapping": not pm,
                "trusted_mapping": any(r["grade"] in ("high", "medium")
                    and r["state"] == "active" and not r["locked"] for r in pm),
                "low_confidence_mapping": any(r["grade"] == "low" and r["state"] == "active" for r in pm),
                "locked_mapping": any(bool(r["locked"]) for r in pm),
                "remote_zero_confirmed": any(r["status"] == "ok" and r["platform_total"] == 0 for r in states),
                "advertised_comments_but_empty": any(r["platform_total"] is not None
                    and r["platform_total"] > 0 for r in states),
                "sync_errors": any(r["status"] == "error" for r in states),
            }
        reasons = {k: 0 for k in ("metadata_absent", "no_snapshot_mapping", "no_platform_mapping",
                    "trusted_mapping", "low_confidence_mapping", "zero_comments", "locked_mapping",
                    "remote_zero_confirmed", "advertised_comments_but_empty", "sync_errors")}
        sources = sorted(set(source_counts) | set(known)
                         | {r["source"] for r in mappings} | {r["source"] for r in sync})
        diagnostics_by_source = {s: dict.fromkeys(("uncovered_tracks", "zero_comments",
            "no_platform_mapping", "trusted_mapping", "low_confidence_mapping", "locked_mapping",
            "remote_zero_confirmed", "advertised_comments_but_empty", "sync_errors"), 0) for s in sources}
        partition = {k: 0 for k in ("metadata_absent", "no_snapshot_mapping", "no_platform_mapping",
                     "trusted_mapping_zero_comments", "low_confidence_mapping_zero_comments",
                     "other_mapping_zero_comments")}
        buckets = dict.fromkeys(_BUCKETS, 0)
        full, missing = [], []
        for guid, track in current.items():
            old_guids = aliases.get(guid, [])
            count = counts.get(guid, 0)
            bucket = ("未匹配" if not old_guids else "0" if count == 0 else "1–20" if count <= 20
                      else "21–100" if count <= 100 else "101–500" if count <= 500 else "500+")
            buckets[bucket] += 1
            row = {"guid": old_guids[0] if len(old_guids) == 1 else guid,
                   "current_guid": guid, "snapshot_guids": old_guids,
                   "song_name": track["title"] or "", "title": track["title"] or "",
                   "eligible": len(old_guids) == 1,
                   "artists": [a["name"] for a in track["artists"]],
                   "artist_details": track["artists"],
                   "album": track["album"] or "", "duration_ms": track["duration_ms"]}
            full.append(row)
            if guid in covered:
                continue
            pm = platform.get(guid, [])
            states = sync_by_current.get(guid, [])
            flags = {"metadata_absent": not _complete(track), "no_snapshot_mapping": not old_guids,
                     "zero_comments": count == 0, **evidence(pm, states)}
            for source, summary in diagnostics_by_source.items():
                source_flags = evidence([r for r in pm if r["source"] == source],
                                        [r for r in states if r["source"] == source])
                summary["uncovered_tracks"] += 1
                summary["zero_comments"] += 1
                for key, value in source_flags.items():
                    summary[key] += int(value)
            row["uncovered_reasons"] = [k for k, v in flags.items() if v]
            for k, v in flags.items():
                reasons[k] += int(v)
            category = next((k for k in ("metadata_absent", "no_snapshot_mapping", "no_platform_mapping") if flags[k]), None)
            if category is None:
                category = ("trusted_mapping_zero_comments" if flags["trusted_mapping"] else
                            "low_confidence_mapping_zero_comments" if flags["low_confidence_mapping"] else
                            "other_mapping_zero_comments")
            partition[category] += 1
            row["uncovered_category"] = category
            missing.append(row)
        total = sum(source_counts.values())
        known_total = sum(known.values())
        return {"available": True, "library_tracks": len(current), "missing_catalog": missing,
                "full_catalog": full, "legacy_snapshot_tracks": len(old), "covered_tracks": len(covered),
                "coverage_ratio": len(covered) / len(current) if current else 0,
                "comments_total": total, "known_platform_comments": known_total,
                "comment_collection_ratio": min(1.0, total / known_total) if known_total else 0,
                "comment_counts_by_source": source_counts, "known_comments_by_source": known,
                "comment_buckets": [{"bucket": k, "tracks": buckets[k]} for k in _BUCKETS],
                "mapped_snapshot_tracks": len(mapped), "ambiguous_snapshot_tracks": ambiguous,
                "covered_by_source": {k: len(v) for k, v in covered_by_source.items()},
                "coverage_diagnostics": {"uncovered_tracks": len(missing),
                    "uncovered_reason_counts": reasons, "reason_counts_overlap": True,
                    "by_source": diagnostics_by_source,
                    "zero_comments_definition": "No stored comments linked to this current-library song; not proof of remote zero.",
                    "remote_evidence_definition": "Sync reports on metadata-linked snapshots; overlapping evidence across aliases/sources, not a mapping-confidence guarantee.",
                    "uncovered_partition": partition, "partition_is_exclusive": True,
                    "snapshot_metadata_absent": sum(not _complete(r) for r in old.values()),
                    "reused_aliases": reused, "invalidated_aliases": invalidated}}
    except (sqlite3.Error, OSError, ValueError, TypeError) as exc:
        return {"available": False, "reason": f"{type(exc).__name__}: {exc}"}
