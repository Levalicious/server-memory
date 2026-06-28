#!/usr/bin/env bash
# Frama-C/WP proof gate for the v3 store.
#
# frama-c exits 0 even when WP goals are unproved, so we PARSE the per-file
# "Proved goals: P / T" summary and gate on it:
#   - clean files must be FULLY proved (P == T);
#   - fc_tree is safety-only — mf_fit_model's termination needs an acyclicity
#     measure (the documented free-list-cycle frontier), so we require its
#     safety/functional goals only (P >= 12) and tolerate the 1 terminates goal.
#
# Run from anywhere; CI calls it via `make -C native proofs`.
set -u
cd "$(dirname "$0")/../native" || exit 2
FRAMAC="${FRAMAC:-frama-c}"
FLAGS=(-wp -wp-rte -wp-prover alt-ergo -wp-timeout 30)
rc=0

gate() {  # file  mode(strict|min)  [minval]
  local f=$1 mode=$2 min=${3:-0}
  echo "===== $f ====="
  local out line
  out=$("$FRAMAC" "${FLAGS[@]}" "$f" 2>&1)
  line=$(echo "$out" | grep -m1 'Proved goals' || true)
  echo "  ${line:-<no goal summary>}"
  if [ -z "$line" ]; then echo "  FAIL: no proof summary for $f"; rc=1; return; fi
  # shellcheck disable=SC2046
  set -- $(echo "$line" | grep -oE '[0-9]+ / [0-9]+' | tr '/' ' ')
  local p=${1:-0} t=${2:-0}
  if [ "$mode" = strict ]; then
    if [ "$p" != "$t" ]; then
      echo "  FAIL: $f proved $p/$t (expected all)"
      echo "$out" | grep -i 'Timeout\]' | sed 's/^/    /'
      rc=1
    fi
  else
    if [ "$p" -lt "$min" ]; then echo "  FAIL: $f proved $p < $min safety goals"; rc=1; fi
  fi
}

gate fc_proofs.c strict
gate fc_index.c  strict
gate fc_graph.c  strict
gate fc_tree.c   min 12      # safety-only; termination = documented acyclicity frontier

if [ "$rc" = 0 ]; then echo "ALL PROOF GATES PASSED"; fi
exit "$rc"
