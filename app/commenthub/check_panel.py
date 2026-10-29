#!/usr/bin/env python3
"""修改 panel.user.js 后的自检：版本号、关键函数、元数据格式、@match 覆盖。"""
import re, sys, os
HERE = os.path.dirname(os.path.abspath(__file__))
src = open(os.path.join(HERE, "panel.user.js"), encoding="utf-8").read()
ok = True

ver = re.search(r"@version\s+([\d.]+)", src)
print(f"版本号        : {ver.group(1) if ver else '缺失'}")

# 只取两个标记之间的元数据块，别把后面的代码/注释也算进来
_m = re.search(r"//\s*==UserScript==(.*?)//\s*==/UserScript==", src, re.S)
meta = _m.group(1) if _m else ""
bad = [l for l in meta.splitlines()
       if l.startswith("//") and not l.startswith("// @") and "UserScript" not in l]
if bad:
    ok = False
    print(f"❌ 元数据块里有非法行: {bad}")
else:
    print("元数据块      : 格式正确")

matches = re.findall(r"@match\s+(\S+)", src)
print(f"@match        : {matches}")
if not any(":5666" in m for m in matches) and not any(":5666" in i for i in re.findall(r"@include\s+(\S+)", src)):
    ok = False; print("❌ 缺少 5666 端口的匹配规则")
if not any(m.startswith("https://") for m in matches):
    ok = False; print("❌ 缺少 https 匹配规则")

for fn in ("collectDiag", "forceOpen", "dockShouldShow", "playerPage", "syncOpen"):
    if f"function {fn}" not in src:
        ok = False; print(f"❌ 缺少函数 {fn}")
print("关键函数      :", "齐全" if ok else "有缺失")

refs = set(re.findall(r"querySelector\(['\"]#([\w-]+)['\"]\)", src))
ids = set(re.findall(r'id="([\w-]+)"', src))
dangling = refs - ids
print("悬空 id 引用  :", dangling or "（无）")
if dangling:
    ok = False

print("\n结果:", "✅ 自检通过" if ok else "❌ 自检失败")
sys.exit(0 if ok else 1)
