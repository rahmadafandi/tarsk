"""Read a side-by-side run and say whether the newest ref got slower.

    python bench/compare.py bench-output.md [--tolerance 0.03]

The report is what `bench.yml` writes when given `refs`: each ref's own `scale`
table, in turn, for two rounds. tarsk's ten-thousand-task median is divided by
taskiq's from the same ref and round, so a runner that was slow for a while
moves both and the quotient stays put. The last ref is compared against the
first, and the run fails only if it is slower by more than the tolerance in
every round: one round is a machine, both are the code.

0.4.0 shipped a 4.6% slowdown that nothing had measured. This is the measuring.
"""

from __future__ import annotations

import re
import sys

HEADING = re.compile(r"## (\S+), round (\d+)")
ROW = re.compile(r"\| (tarsk|taskiq) \(streams, acked\)(?: `slow`)? \| 10,000 \| [\d.]+ \| ([\d.]+)")


def read(text: str) -> dict[str, dict[str, dict[str, float]]]:
    """ref -> round -> {"tarsk": seconds, "taskiq": seconds}, in the order run."""
    seen: dict[str, dict[str, dict[str, float]]] = {}
    ref = rnd = ""
    for line in text.splitlines():
        if at := HEADING.match(line):
            ref, rnd = at.groups()
        elif ref and (row := ROW.match(line)):
            seen.setdefault(ref, {}).setdefault(rnd, {})[row[1]] = float(row[2])
    return seen


def verdict(seen: dict, tolerance: float) -> tuple[list[str], bool]:
    """The table to print, and whether the last ref is slower than the first."""
    refs = list(seen)
    if len(refs) < 2:
        raise SystemExit("need at least two refs with a `scale` table each to compare")
    base = seen[refs[0]]
    lines = ["| ref | round | tarsk | taskiq | tarsk ÷ taskiq | against " + refs[0] + " |",
             "|---|---|---|---|---|---|"]
    changes: dict[str, list[float]] = {}
    for ref in refs:
        for rnd, row in seen[ref].items():
            if set(row) != {"tarsk", "taskiq"} or set(base.get(rnd, ())) != {"tarsk", "taskiq"}:
                raise SystemExit(f"{ref}, round {rnd}: no drain figures for both runtimes")
            quotient = row["tarsk"] / row["taskiq"]
            change = quotient / (base[rnd]["tarsk"] / base[rnd]["taskiq"]) - 1
            changes.setdefault(ref, []).append(change)
            lines.append(f"| {ref} | {rnd} | {row['tarsk']:.3f} | {row['taskiq']:.3f} | "
                         f"{quotient:.3f} | {change:+.1%} |")
    slower = all(change > tolerance for change in changes[refs[-1]])
    lines.append("")
    lines.append(
        f"**{refs[-1]} is slower than {refs[0]} by more than {tolerance:.0%} in every round.**"
        if slower else
        f"{refs[-1]} is within {tolerance:.0%} of {refs[0]}, or faster, in at least one round.")
    return lines, slower


def demo() -> None:
    def report(*rows):
        return "\n".join(
            f"## {ref}, round {rnd}\n"
            f"| taskiq (streams, acked) | 10,000 | 0 | {taskiq} | 0 | 0 | 0 |\n"
            f"| tarsk (streams, acked){mark} | 10,000 | 0 | {tarsk} | 0 | 0 | 0 |"
            for ref, rnd, tarsk, taskiq, mark in rows)

    # What 0.3.0 against 0.4.0 read on the runner: slower in both rounds.
    _, slower = verdict(read(report(("0.3.0", 1, 1.151, 1.785, ""), ("0.4.0", 1, 1.205, 1.776, ""),
                                    ("0.3.0", 2, 1.149, 1.786, ""), ("0.4.0", 2, 1.201, 1.789, " `slow`"))), 0.03)
    assert slower
    # A runner that slowed for one round moves both runtimes, and is not the code.
    _, slower = verdict(read(report(("a", 1, 1.00, 2.00, ""), ("b", 1, 1.30, 2.60, ""),
                                    ("a", 2, 1.00, 2.00, ""), ("b", 2, 1.01, 2.00, ""))), 0.03)
    assert not slower
    # Slower in one round only is a machine.
    _, slower = verdict(read(report(("a", 1, 1.00, 2.00, ""), ("b", 1, 1.10, 2.00, ""),
                                    ("a", 2, 1.00, 2.00, ""), ("b", 2, 1.00, 2.00, ""))), 0.03)
    assert not slower


if __name__ == "__main__":
    if sys.argv[1:] == ["--demo"]:
        demo()
        sys.exit(0)
    tolerance = float(sys.argv[sys.argv.index("--tolerance") + 1]) if "--tolerance" in sys.argv else 0.03
    with open(sys.argv[1], encoding="utf-8") as report:
        table, slower = verdict(read(report.read()), tolerance)
    print("\n".join(table))
    sys.exit(1 if slower else 0)
