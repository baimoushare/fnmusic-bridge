#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
复核已有映射：把「平台歌名带版本标记（伴奏/live/remix/翻唱…）而本地曲名没有」的映射降级。

harvest 复用历史报告时，只带了当时的分数与状态，没有重新做版本一致性检查，
所以会漏进少量「本地是原曲、映射到平台伴奏版」这类错配。
评论张冠李戴比没有评论更糟，所以这里做一次全量复核。

用法：
  python3 verify.py            # 预演，列出会被降级的
  python3 verify.py --apply    # 真正降级
"""

import argparse
import json
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matching as M   # noqa: E402
import store as S      # noqa: E402


# 本地歌手字段是这些「非歌手」占位时，不拿它做判断
PLACEHOLDER_ARTISTS = {"群星", "va", "variousartists", "未知", "unknown", "未知艺术家", "佚名", ""}


def check_pair(local_title, src_title, src_artists, local_artists):
    """返回 (ok, reason)。

    分级处理，避免误杀：
      - 版本冲突（live/伴奏/remix 对不上）         -> 不可信，降级
      - 平台多出伴奏/纯音乐等版本词                -> 不可信，降级
      - 标题一致但歌手不同（可能是繁简/别名/同名翻唱）-> 存疑，进人工复核但保留
      - 标题只是近似且歌手不同                    -> 不可信，降级
    """
    if M.version_conflict(local_title, src_title):
        a, b = M.version_tags(local_title), M.version_tags(src_title)
        return False, "版本冲突: 本地%s vs 平台%s" % (sorted(a) or "无", sorted(b) or "无")
    # 平台歌名比本地多了版本词（本地没写但其实是伴奏的情况）
    extra = M.version_tags(src_title) - M.version_tags(local_title)
    if extra:
        return False, "平台带版本标记: " + "/".join(sorted(extra))

    la, sa = M.artists_of({"artists": local_artists}), M.artists_of({"artists": src_artists})
    la = {a for a in la if a not in PLACEHOLDER_ARTISTS}
    if la and sa and not (la & sa) and not any(x in y or y in x for x in la for y in sa):
        # 标题是否严格一致？一致的话更可能是繁简/别名，不能直接判错
        exact = (M.norm_title(local_title) == M.norm_title(src_title)
                 or M.core_title(local_title) == M.core_title(src_title))
        if exact:
            return "suspect", "标题一致但歌手不同（可能是别名/繁简/翻唱）"
        return False, "标题近似且歌手不一致"
    return True, "ok"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()

    con = S.init()
    rows = con.execute("""
        SELECT m.track_guid, m.source, m.song_id, m.song_name, m.song_artist, m.grade,
               t.title AS local_title, t.artists AS local_artists
        FROM mappings m JOIN tracks t ON t.guid = m.track_guid
        WHERE m.grade IN ('high','medium') AND m.locked = 0
    """).fetchall()
    rows = [dict(r) for r in rows]
    if args.limit:
        rows = rows[:args.limit]

    bad, suspect = [], []
    for r in rows:
        local_artists = json.loads(r["local_artists"] or "[]")
        ok, why = check_pair(r["local_title"], r["song_name"] or "",
                             (r["song_artist"] or "").split("/"), local_artists)
        if ok is False:
            bad.append((r, why))
        elif ok == "suspect":
            suspect.append((r, why))

    print(f"复核 {len(rows)} 条映射：确定可疑 {len(bad)} 条（{len(bad)/max(len(rows),1):.1%}），"
          f"存疑 {len(suspect)} 条（保留但进人工复核）")
    for r, why in bad[:20]:
        print(f"  [{r['source']}] {r['local_title'][:24]:<24} -> {r['song_name'][:24]:<24} {why}")
    if len(bad) > 20:
        print(f"  … 另有 {len(bad)-20} 条")

    if not args.apply:
        print("\n（预演。加 --apply 把可疑映射降级为 low，面板就不再展示这些评论）")
        return

    for r, why in bad:
        con.execute("""UPDATE mappings SET grade='low',
                       reasons=json_insert(COALESCE(reasons,'[]'), '$[#]', ?)
                       WHERE track_guid=? AND source=?""",
                    (f"复核降级: {why}", r["track_guid"], r["source"]))
    con.commit()
    print(f"\n已降级 {len(bad)} 条；{len(suspect)} 条存疑的保留原等级，可在面板上人工核对。")


if __name__ == "__main__":
    main()
