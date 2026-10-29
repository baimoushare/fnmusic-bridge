#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""静态检查：模块里"被引用但未定义"的全局名字（防大段替换误删常量/函数）。

动机（真实事故 2026-09-28）：一次大段文本替换把 `apiv2.py` 里的 `ERR_CODES`、
`_artifact`、`loader_with_update_url` **一起删掉了**。它们在错误路径/新端点上才被用到，
所以：
  * 单独跑通过的测试没覆盖到（例如只跑健康检查）；
  * 如果测试没跑，问题会以 `NameError` 的形式在**运行时**炸出来（500 / 连接被断开）。

本检查只看语法层，不执行代码、不依赖第三方库；把"少了个名字"变成**提交前就能发现**的事。

用法：
    python3 -B tools/check_undefined_names.py [文件...]     # 默认检查本目录的 *.py 与 tools/*.py
退出码：0 = 没发现；1 = 发现可疑名字。
"""

import ast
import builtins
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def module_globals(tree):
    names = set(dir(builtins)) | {"__name__", "__file__", "__doc__", "__package__"}
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            names.add(node.name)
        elif isinstance(node, ast.Assign):
            for t in node.targets:
                names.update(_targets(t))
        elif isinstance(node, ast.AnnAssign) and node.target is not None:
            names.update(_targets(node.target))
        elif isinstance(node, (ast.Import, ast.ImportFrom)):
            for a in node.names:
                names.add(a.asname or a.name.split(".")[0])
        elif isinstance(node, ast.Try):                    # try: import x except: pass
            for sub in ast.walk(node):
                if isinstance(sub, (ast.Import, ast.ImportFrom)):
                    for a in sub.names:
                        names.add(a.asname or a.name.split(".")[0])
    names |= _module_level_bindings(tree)
    return names


def _module_level_bindings(tree):
    """模块级代码里"绑定名字"的各种写法：for/with 目标、推导式变量、异常别名。

    只走模块顶层语句，不进函数/类体（那些属于各自作用域）。
    """
    out = set()

    def visit(node):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Lambda)):
            return
        if isinstance(node, ast.Assign):
            for t in node.targets:
                out.update(_targets(t))
        elif isinstance(node, ast.AnnAssign) and node.target is not None:
            out.update(_targets(node.target))
        elif isinstance(node, (ast.AugAssign,)):
            out.update(_targets(node.target))
        elif isinstance(node, ast.ExceptHandler) and node.name:
            out.add(node.name)
        elif isinstance(node, (ast.For, ast.AsyncFor)):
            out.update(_targets(node.target))
        elif isinstance(node, ast.With):
            for item in node.items:
                if item.optional_vars is not None:
                    out.update(_targets(item.optional_vars))
        elif isinstance(node, (ast.comprehension,)):
            out.update(_targets(node.target))
        for child in ast.iter_child_nodes(node):
            visit(child)

    for stmt in tree.body:
        visit(stmt)
    return out


def _targets(node):
    if isinstance(node, ast.Name):
        return {node.id}
    if isinstance(node, (ast.Tuple, ast.List)):
        out = set()
        for e in node.elts:
            out |= _targets(e)
        return out
    return set()


def local_names(fn):
    out = set()
    for a in list(fn.args.args) + list(fn.args.kwonlyargs) + list(fn.args.posonlyargs):
        out.add(a.arg)
    if fn.args.vararg:
        out.add(fn.args.vararg.arg)
    if fn.args.kwarg:
        out.add(fn.args.kwarg.arg)
    for node in ast.walk(fn):
        if isinstance(node, ast.Name) and isinstance(node.ctx, (ast.Store, ast.Del)):
            out.add(node.id)
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            out.add(node.name)                     # 嵌套 def/class 也是本作用域的名字
        elif isinstance(node, (ast.Import, ast.ImportFrom)):
            for a in node.names:
                out.add(a.asname or a.name.split(".")[0])
        elif isinstance(node, ast.ExceptHandler) and node.name:
            out.add(node.name)
        elif isinstance(node, (ast.comprehension,)):
            if isinstance(node.target, ast.Name):
                out.add(node.target.id)
    return out


def check_file(path):
    """按**作用域栈**检查：函数内的引用可以是本人局部、任一外层函数的局部、或模块全局。

    早先的版本只看"本函数局部 + 模块全局"，于是把闭包引用的外层变量（如 `log`、`stats`）
    全报成"未定义" —— 假阳性比漏报更浪费注意力，所以这里显式处理嵌套作用域。
    """
    with open(path, encoding="utf-8") as fh:
        tree = ast.parse(fh.read(), filename=path)
    globs = module_globals(tree)
    problems = []

    def walk(node, scopes):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            walk_children(node, scopes + [local_names(node)])
            return
        if isinstance(node, ast.ClassDef):
            body_names = set()
            for stmt in node.body:
                if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                    body_names.add(stmt.name)
                elif isinstance(stmt, ast.Assign):
                    for t in stmt.targets:
                        body_names.update(_targets(t))
                elif isinstance(stmt, ast.AnnAssign) and stmt.target is not None:
                    body_names.update(_targets(stmt.target))
            walk_children(node, scopes + [body_names])
            return
        if isinstance(node, ast.Lambda):
            loc = {a.arg for a in list(node.args.args) + list(node.args.kwonlyargs)
                   + list(node.args.posonlyargs)}
            if node.args.vararg:
                loc.add(node.args.vararg.arg)
            if node.args.kwarg:
                loc.add(node.args.kwarg.arg)       # lambda src, **kw: ... **kw
            walk_children(node, scopes + [loc])
            return
        if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load):
            if node.id not in globs and not any(node.id in sc for sc in scopes):
                problems.append((node.lineno, node.id))
        walk_children(node, scopes)

    def walk_children(node, scopes):
        for child in ast.iter_child_nodes(node):
            walk(child, scopes)

    walk_children(tree, [])
    # 同一个名字重复出现只报一次（报第一个位置），避免刷屏
    seen, uniq = set(), []
    for line, name in problems:
        if name in seen:
            continue
        seen.add(name)
        uniq.append((line, name))
    return uniq


def main(argv):
    targets = argv[1:]
    if not targets:
        targets = sorted(
            [os.path.join(ROOT, f) for f in os.listdir(ROOT) if f.endswith(".py")]
            + [os.path.join(ROOT, "tools", f)
               for f in os.listdir(os.path.join(ROOT, "tools")) if f.endswith(".py")]
            + [os.path.join(ROOT, "adapters", f)
               for f in os.listdir(os.path.join(ROOT, "adapters")) if f.endswith(".py")]
        )
    bad = 0
    for path in targets:
        try:
            problems = check_file(path)
        except SyntaxError as e:
            print(f"❌ 语法错误 {path}: {e}")
            bad += 1
            continue
        if problems:
            bad += 1
            rel = os.path.relpath(path, ROOT)
            print(f"❌ {rel}：{len(problems)} 处引用了未定义的名字")
            for line, name in problems[:20]:
                print(f"     {rel}:{line} 引用了未定义的 `{name}`")
    if bad:
        print(f"\n共 {bad} 个文件有问题（多半是大段替换误删了定义）")
        return 1
    print(f"✅ 检查 {len(targets)} 个文件，未发现未定义的名字")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
