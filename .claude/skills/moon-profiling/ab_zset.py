#!/usr/bin/env python3
import re
import statistics
import subprocess
import sys

# usage: ab_zset.py [rounds] [base_binary] [other_binary]
ROUNDS = int(sys.argv[1]) if len(sys.argv) > 1 else 5
BASE = sys.argv[2] if len(sys.argv) > 2 else "/tmp/moon_base"
OTHER = sys.argv[3] if len(sys.argv) > 3 else "/tmp/moon_pf"
BINS = {"base": BASE, "prefetch": OTHER}
SCRIPT = "assets/benchmark/benchmark_zset.lua"
OPS = ["build(insert)", "rank", "key_by_rank", "update(reposition)", "erase", "reinsert"]

pat = re.compile(r"\[([\w()]+)\s*\]\s+\d+ ops in\s+([\d.]+) ms")
data = {k: {op: [] for op in OPS} for k in BINS}

for _ in range(ROUNDS):
    for name, binpath in BINS.items():  # interleave base/prefetch each round
        out = subprocess.run([binpath, SCRIPT, "1000000", "1000000"],
                             capture_output=True, text=True).stdout
        seen = set()
        for m in pat.finditer(out):
            op, ms = m.group(1), float(m.group(2))
            if op in data[name] and op not in seen:
                data[name][op].append(ms)
                seen.add(op)

print(f"rounds={ROUNDS}  (total ms, lower=better)\n")
print(f"{'op':20} {'base_mean':>10} {'pf_mean':>9} {'Δmean':>7}   {'base_min':>9} {'pf_min':>8} {'Δmin':>7}")
print("-" * 78)
for op in OPS:
    b = data["base"][op]
    p = data["prefetch"][op]
    if not b or not p:
        continue
    mb, mp = statistics.mean(b), statistics.mean(p)
    nb, np_ = min(b), min(p)
    dmean = (mp - mb) / mb * 100
    dmin = (np_ - nb) / nb * 100
    print(f"{op:20} {mb:10.1f} {mp:9.1f} {dmean:+6.1f}%   {nb:9.1f} {np_:8.1f} {dmin:+6.1f}%")
