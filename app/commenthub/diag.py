#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DOM 报告收集：用来搞清楚飞牛音乐前端**真实**的界面结构，而不是靠猜选择器。

面板里点「收集界面结构」后，油猴脚本会把当前页面的
  · 路由（pathname/hash）、视口尺寸
  · 各级容器的 class / id / 尺寸 / position / display
  · 播放条、主内容区、歌词区等关键节点的实测样式
POST 到 /api/diag，落到 commenthub 目录下的 diag_*.json
"""
import json
import os
import time

HERE = os.path.dirname(os.path.abspath(__file__))


def save(report):
    """保存一份前端结构报告，返回文件名"""
    ts = time.strftime("%Y%m%d-%H%M%S")
    name = f"diag_{ts}.json"
    path = os.path.join(HERE, name)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=1)
    latest = os.path.join(HERE, "diag_latest.json")
    try:
        os.remove(latest)
    except OSError:
        pass
    try:
        os.symlink(name, latest)
    except OSError:
        with open(latest, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=1)
    return name


def summarize(path=None):
    """把报告浓缩成人类可读的结论"""
    path = path or os.path.join(HERE, "diag_latest.json")
    if not os.path.exists(path):
        return "还没有收到界面结构报告"
    r = json.load(open(path, encoding="utf-8"))
    out = [f"路由: {r.get('url')}",
           f"视口: {r.get('viewport')}",
           f"协议: {r.get('protocol')}",
           f"收到时间: {r.get('receivedAt')}"]
    out.append("\n候选容器（按面积从大到小）:")
    for n in (r.get("nodes") or [])[:25]:
        out.append("  {tag}.{cls} id={id} {w}x{h} pos={pos} disp={disp} overflow={ovf}".format(
            tag=n.get("tag"), cls=(n.get("cls") or "")[:60], id=n.get("id") or "-",
            w=n.get("w"), h=n.get("h"), pos=n.get("position"), disp=n.get("display"),
            ovf=n.get("overflowY")))
    if r.get("fixed"):
        out.append("\nposition:fixed 的节点（播放条通常在这里）:")
        for n in r["fixed"][:15]:
            out.append("  {tag}.{cls} {w}x{h} top={top} bottom={bottom} z={z}".format(
                tag=n.get("tag"), cls=(n.get("cls") or "")[:60], w=n.get("w"), h=n.get("h"),
                top=n.get("top"), bottom=n.get("bottom"), z=n.get("zIndex")))
    return "\n".join(out)


if __name__ == "__main__":
    print(summarize())
