#!/usr/bin/env python3
"""Parse a samply / Firefox-profiler JSON(.gz) and print self-time by function.

Usage: python3 parse_profile.py profile.json.gz [top_n]
"""
import gzip
import json
import sys
from collections import defaultdict


def load(path):
    op = gzip.open if path.endswith(".gz") else open
    with op(path, "rt") as f:
        return json.load(f)


def get_string_array(thread):
    # Newer format: top-level profile.shared.stringArray; older: thread.stringTable / stringArray
    for k in ("stringArray", "stringTable"):
        if k in thread:
            return thread[k]
    return None


def main():
    path = sys.argv[1]
    top_n = int(sys.argv[2]) if len(sys.argv) > 2 else 40
    prof = load(path)

    shared_strings = None
    if "shared" in prof and "stringArray" in prof["shared"]:
        shared_strings = prof["shared"]["stringArray"]

    self_time = defaultdict(float)
    total_samples = 0.0

    for thread in prof["threads"]:
        strings = get_string_array(thread) or shared_strings
        if strings is None:
            continue
        func_table = thread["funcTable"]
        frame_table = thread["frameTable"]
        stack_table = thread["stackTable"]
        samples = thread["samples"]

        func_name_idx = func_table["name"]  # func -> string index
        frame_func = frame_table["func"]    # frame -> func index
        stack_frame = stack_table["frame"]  # stack -> frame index

        sample_stacks = samples["stack"]
        weights = samples.get("weight")
        n = samples.get("length", len(sample_stacks))

        for i in range(n):
            st = sample_stacks[i]
            if st is None:
                continue
            w = weights[i] if weights is not None else 1
            if w is None:
                w = 1
            frame = stack_frame[st]
            func = frame_func[frame]
            name = strings[func_name_idx[func]]
            self_time[name] += w
            total_samples += w

    print(f"total weighted samples: {total_samples:.0f}\n")
    print(f"{'self%':>7}  {'samples':>10}  function")
    print("-" * 80)
    ranked = sorted(self_time.items(), key=lambda kv: kv[1], reverse=True)
    for name, w in ranked[:top_n]:
        pct = 100.0 * w / total_samples if total_samples else 0.0
        print(f"{pct:7.2f}  {w:10.0f}  {name}")


if __name__ == "__main__":
    main()
