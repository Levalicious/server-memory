# Dev-time test build for the v3 C store.
#
# Compiles the fuzz/unit harnesses with AddressSanitizer + UBSan AND the
# test-only double-free detector (-DMEMFILE_DOUBLE_FREE_CHECK). The PUBLISHED
# addon is built by binding.gyp / node-gyp, which NEVER defines that macro, so
# the detector is physically absent from the shipped binary (the production
# allocator stays clean — Principle_FixCauseNotChecks).
CC ?= cc
CFLAGS = -std=c11 -g -O1 -Wall -Wextra -fsanitize=address,undefined \
         -D_GNU_SOURCE -DMEMFILE_DOUBLE_FREE_CHECK -I.
LIBS = -lm
OUT ?= /tmp/mf_test.$(shell echo $$PPID)

.PHONY: test verify-detector test_memfile test_stringtable test_graph test_entity test_segmeta test_segpage test_segfile test_segtxn test_mstore test_str4 test_graph4 test_parity test_daemon test_import test_riblt test_repl test_segtree test_regex regex_cli bench bench_regex bench_sheng bench_widesheng proofs proofs-eva wp-segmeta clean

test_riblt: test_riblt.c riblt.c blake2s.c
	$(CC) $(CFLAGS) $^ $(LIBS) -o $(OUT)_riblt && $(OUT)_riblt

REPL_SRC = test_repl.c seg_repl.c riblt.c blake2s.c seg_tree.c seg_graph4.c seg_str4.c seg_mstore.c seg_txn.c seg_file.c seg_io.c seg_meta.c seg_page.c regex.c re_dfa.c re_trigram.c
test_repl: $(REPL_SRC)
	$(CC) $(CFLAGS) $(REPL_SRC) $(LIBS) -o $(OUT)_repl && $(OUT)_repl

# `make test` = prove the detector fires, then run every harness with it active.
test: verify-detector test_memfile test_stringtable test_graph test_entity test_segmeta test_segpage test_segfile test_segtxn test_mstore test_str4 test_graph4 test_segtree test_parity test_daemon test_import test_riblt test_repl test_regex

test_segmeta: test_segmeta.c seg_meta.c
	$(CC) $(CFLAGS) $^ $(LIBS) -o $(OUT)_segmeta && $(OUT)_segmeta

test_segpage: test_segpage.c seg_page.c
	$(CC) $(CFLAGS) $^ $(LIBS) -o $(OUT)_segpage && $(OUT)_segpage

# WP + RTE over the slotted page module (libsegstore proof target #2).
wp-segpage:
	frama-c -wp -wp-rte -wp-timeout 20 seg_page.c -cpp-extra-args="-I."

# WP + RTE over the dual-meta recovery pivot (libsegstore proof target #1).
# The sole assumed obligation is meta_body_crc's ensures (crc trust boundary).
wp-segmeta:
	frama-c -wp -wp-rte -wp-timeout 20 seg_meta.c -cpp-extra-args="-I."

verify-detector: test_doublefree.c memoryfile.c
	@$(CC) $(CFLAGS) test_doublefree.c memoryfile.c $(LIBS) -o $(OUT)_df
	@if $(OUT)_df 2>&1 | grep -q "DOUBLE-FREE detected"; then \
	  echo "ok:   double-free detector fires (abort) on a deliberate double-free"; \
	else echo "FAIL: double-free detector did NOT fire"; exit 1; fi

test_memfile: test_memfile.c memoryfile.c
	$(CC) $(CFLAGS) $^ $(LIBS) -o $(OUT)_memfile && $(OUT)_memfile

test_stringtable: test_stringtable.c stringtable.c memoryfile.c
	$(CC) $(CFLAGS) $^ $(LIBS) -o $(OUT)_st && $(OUT)_st

test_graph: test_graph.c graph.c stringtable.c memoryfile.c regex.c re_dfa.c re_trigram.c
	$(CC) $(CFLAGS) $^ $(LIBS) -o $(OUT)_graph && $(OUT)_graph

test_entity: test_entity.c
	$(CC) $(CFLAGS) $^ $(LIBS) -o $(OUT)_entity && $(OUT)_entity

# Per-op graph benchmark: optimized build (NO ASan / NO double-free-check — those
# skew timing). Emits per-op rdtsc cycle stats as JSON; CI compares base vs head.
BENCH_CFLAGS = -std=c11 -O2 -march=native -Wall -D_GNU_SOURCE -I.
bench: op_bench.c graph.c stringtable.c memoryfile.c regex.c re_dfa.c re_trigram.c
	$(CC) $(BENCH_CFLAGS) $^ -lm -o $(OUT)_bench && $(OUT)_bench

# ---- Regex engine: standalone tests + explorers (ported from feat/regex-engine) ----
# Engine + determinizer + Sheng + lazy Sheng (standalone). ASan+UBSan; -mssse3 for PSHUFB.
test_regex: test_regex.c regex.c re_dfa.c re_sheng.c re_lsheng.c
	$(CC) $(CFLAGS) -mssse3 $^ $(LIBS) -o $(OUT)_regex && $(OUT)_regex

# CLI driver for the Python differential fuzzer (scripts/regex_diff.py):
# pattern = argv[1], text read from stdin; exit 0 = match, 1 = no match, 2 = error.
# REGEX_ENGINE=nfa|dfa|sheng|ldfa|lsheng selects the engine, so the fuzzer oracles all.
regex_cli: regex_cli.c regex.c re_dfa.c re_sheng.c re_lsheng.c
	$(CC) $(CFLAGS) -mssse3 $^ $(LIBS) -o $(OUT)_regex_cli

# Regex compile + match microbench (rdtsc, adaptive sampling; same conventions
# as op_bench). Emits per-case cycle stats as JSON.
bench_regex: bench_regex.c regex.c
	$(CC) $(BENCH_CFLAGS) $^ -lm -o $(OUT)_regex_bench && $(OUT)_regex_bench

# Quiet-Sheng (PSHUFB DFA) vs scalar BasicDFA throughput. Needs SSSE3.
bench_sheng: bench_sheng.c re_sheng.c re_dfa.c regex.c
	$(CC) $(BENCH_CFLAGS) -mssse3 $^ -lm -o $(OUT)_sheng_bench && $(OUT)_sheng_bench

# Cost of scaling Sheng past 16 states (general-gather, N/16 PSHUFB/byte). SSE4.1 for blendv.
bench_widesheng: bench_widesheng.c
	$(CC) $(BENCH_CFLAGS) -msse4.1 $^ -lm -o $(OUT)_widesheng_bench && $(OUT)_widesheng_bench

# ---- Frama-C/WP + EVA proofs ----------------------------------------------
# Memory-model-clean abstractions in fc_*.c (NEVER compiled into the build):
# allocator size-quantization + open-addressing probe (fc_proofs), name-index
# traversal bounds (fc_index), free-list descent safety (fc_tree), adjacency
# edge encoding + edge-iteration bounds (fc_graph), and an EVA pass over the
# real allocator (fc_harness). frama-c + alt-ergo + z3 (CI installs via opam).
FRAMAC ?= frama-c

# frama-c exits 0 even with unproved goals, so the gate (scripts/run-proofs.sh)
# parses the "Proved goals: P / T" summary: clean files must be 100%, fc_tree is
# safety-only (termination = documented acyclicity frontier).
proofs:
	@FRAMAC="$(FRAMAC)" bash ../scripts/run-proofs.sh

proofs-eva: fc_harness.c memoryfile.c
	$(FRAMAC) -eva -main fc_main -cpp-extra-args="-include fc_stubs.h -I." $^

clean:
	rm -f $(OUT)_*

test_segfile: test_segfile.c seg_file.c seg_io.c seg_meta.c seg_page.c
	$(CC) $(CFLAGS) $^ $(LIBS) -o $(OUT)_segfile && $(OUT)_segfile

test_segtxn: test_segtxn.c seg_txn.c seg_file.c seg_io.c seg_meta.c seg_page.c
	$(CC) $(CFLAGS) $^ $(LIBS) -o $(OUT)_segtxn && $(OUT)_segtxn

test_mstore: test_mstore.c seg_mstore.c seg_txn.c seg_file.c seg_io.c seg_meta.c seg_page.c
	$(CC) $(CFLAGS) $^ $(LIBS) -o $(OUT)_mstore && $(OUT)_mstore

SEGBENCH_SRC = bench_segstore.c seg_txn.c seg_file.c seg_io.c seg_meta.c seg_page.c
bench_segstore: $(SEGBENCH_SRC)
	$(CC) $(BENCH_CFLAGS) $(SEGBENCH_SRC) -lm -o $(OUT)_segbench4k && $(OUT)_segbench4k
	$(CC) $(BENCH_CFLAGS) -DSEG_PAGE_SIZE=16384u $(SEGBENCH_SRC) -lm -o $(OUT)_segbench16k && $(OUT)_segbench16k

test_str4: test_str4.c seg_str4.c seg_txn.c seg_file.c seg_io.c seg_meta.c seg_page.c
	$(CC) $(CFLAGS) $^ $(LIBS) -o $(OUT)_str4 && $(OUT)_str4

test_graph4: test_graph4.c seg_tree.c seg_graph4.c seg_str4.c seg_mstore.c seg_txn.c seg_file.c seg_io.c seg_meta.c seg_page.c regex.c re_dfa.c re_trigram.c seg_meta.c
	$(CC) $(CFLAGS) $^ $(LIBS) -o $(OUT)_graph4 && $(OUT)_graph4

test_parity: test_parity.c graph.c stringtable.c memoryfile.c seg_tree.c seg_graph4.c seg_str4.c seg_mstore.c seg_txn.c seg_file.c seg_io.c seg_meta.c seg_page.c regex.c re_dfa.c re_trigram.c
	$(CC) $(CFLAGS) $^ $(LIBS) -o $(OUT)_parity && $(OUT)_parity

IMPORT_SRC = v4_import.c graph.c stringtable.c memoryfile.c seg_tree.c seg_graph4.c seg_str4.c seg_mstore.c seg_txn.c seg_file.c seg_io.c seg_meta.c seg_page.c regex.c re_dfa.c re_trigram.c
v4_import: $(IMPORT_SRC)
	$(CC) $(BENCH_CFLAGS) $(IMPORT_SRC) -lm -o v4_import

test_import: test_import.c graph.c stringtable.c memoryfile.c $(DAEMON_SRC)
	$(CC) $(CFLAGS) test_import.c graph.c stringtable.c memoryfile.c $(DAEMON_SRC) $(LIBS) -o $(OUT)_import && $(OUT)_import

test_segtree: test_segtree.c $(DAEMON_SRC)
	$(CC) $(CFLAGS) test_segtree.c $(DAEMON_SRC) $(LIBS) -o $(OUT)_segtree && $(OUT)_segtree

DAEMON_SRC = seg_tree.c seg_graph4.c seg_str4.c seg_mstore.c seg_txn.c seg_file.c seg_io.c seg_meta.c seg_page.c regex.c re_dfa.c re_trigram.c riblt.c blake2s.c
kbd4: seg_daemon.c $(DAEMON_SRC)
	$(CC) -std=c11 -O2 -march=native -Wall -Wextra -D_GNU_SOURCE -I. $^ $(LIBS) -o kbd4

test_daemon: test_daemon.c $(DAEMON_SRC)
	$(CC) $(CFLAGS) $^ $(LIBS) -o $(OUT)_daemon && $(OUT)_daemon
