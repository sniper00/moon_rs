#!/usr/bin/env python3
"""Symbolicate a samply profile (.json.gz) with atos and print self-time by function.

Usage: python3 analyze_samply.py <profile.json.gz> <binary> [thread_name] [top_n]
"""
import gzip
import json
import re
import subprocess
import sys
from collections import defaultdict

TEXT_BASE = 0x100000000


def load(path):
    op = gzip.open if path.endswith(".gz") else open
    with op(path, "rt") as f:
        return json.load(f)


def demangle(name):
    # strip rust hash suffix ::h<hex>
    return re.sub(r"::h[0-9a-f]{16}$", "", name)


def main():
    prof_path = sys.argv[1]
    binary = sys.argv[2]
    thread_name = sys.argv[3] if len(sys.argv) > 3 else "actor-bootstrap"
    top_n = int(sys.argv[4]) if len(sys.argv) > 4 else 35

    prof = load(prof_path)
    threads = [t for t in prof["threads"] if t.get("name") == thread_name]
    if not threads:
        print("thread not found:", thread_name)
        print("available:", sorted({t.get("name") for t in prof["threads"]}))
        return
    th = threads[0]
    S = th["stringArray"]
    ft, frt, stt, sm = th["funcTable"], th["frameTable"], th["stackTable"], th["samples"]
    rtbl = th["resourceTable"]
    libidx = rtbl["lib"]
    main_lib = next(i for i, l in enumerate(prof["libs"]) if l.get("name") == "moon_rs")

    # leaf self counts by (lib, reladdr-string)
    selfc = defaultdict(int)
    stacks = sm["stack"]
    n = sm["length"]
    for i in range(n):
        st = stacks[i]
        if st is None:
            continue
        fr = stt["frame"][st]
        fn = frt["func"][fr]
        res = ft["resource"][fn]
        lib = libidx[res] if res >= 0 else -1
        addr = S[ft["name"][fn]]
        selfc[(lib, addr)] += 1

    total = sum(selfc.values())

    # collect moon_rs addresses to symbolicate
    moon_addrs = [addr for (lib, addr) in selfc if lib == main_lib and addr.startswith("0x")]
    sym = {}
    if moon_addrs:
        svmas = [hex(TEXT_BASE + int(a, 16)) for a in moon_addrs]
        out = subprocess.run(
            ["atos", "-o", binary, "-l", hex(TEXT_BASE), *svmas],
            capture_output=True, text=True,
        ).stdout.splitlines()
        for a, line in zip(moon_addrs, out):
            sym[a] = line.strip()

    # aggregate by symbol name
    by_func = defaultdict(int)
    for (lib, addr), c in selfc.items():
        if lib == main_lib and addr in sym:
            raw = sym[addr]
            m = re.match(r"^(.*?)\s+\(in ", raw)
            name = m.group(1) if m else raw
            name = demangle(name)
        elif lib == -1:
            name = "[unknown]"
        else:
            libname = prof["libs"][lib].get("name", f"lib{lib}") if lib >= 0 else "?"
            name = f"{libname}!{addr}"
        by_func[name] += c

    print(f"thread={thread_name}  total leaf samples={total}\n")
    print(f"{'self%':>7}  {'samples':>8}  function")
    print("-" * 90)
    for name, c in sorted(by_func.items(), key=lambda kv: kv[1], reverse=True)[:top_n]:
        print(f"{100*c/total:7.2f}  {c:8d}  {name}")


if __name__ == "__main__":
    main()
