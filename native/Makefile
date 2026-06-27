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
OUT = /tmp/mf_test

.PHONY: test verify-detector test_memfile test_stringtable test_graph test_entity bench clean

# `make test` = prove the detector fires, then run every harness with it active.
test: verify-detector test_memfile test_stringtable test_graph test_entity

verify-detector: test_doublefree.c memoryfile.c
	@$(CC) $(CFLAGS) test_doublefree.c memoryfile.c $(LIBS) -o $(OUT)_df
	@if $(OUT)_df 2>&1 | grep -q "DOUBLE-FREE detected"; then \
	  echo "ok:   double-free detector fires (abort) on a deliberate double-free"; \
	else echo "FAIL: double-free detector did NOT fire"; exit 1; fi

test_memfile: test_memfile.c memoryfile.c
	$(CC) $(CFLAGS) $^ $(LIBS) -o $(OUT)_memfile && $(OUT)_memfile

test_stringtable: test_stringtable.c stringtable.c memoryfile.c
	$(CC) $(CFLAGS) $^ $(LIBS) -o $(OUT)_st && $(OUT)_st

test_graph: test_graph.c graph.c stringtable.c memoryfile.c
	$(CC) $(CFLAGS) $^ $(LIBS) -o $(OUT)_graph && $(OUT)_graph

test_entity: test_entity.c
	$(CC) $(CFLAGS) $^ $(LIBS) -o $(OUT)_entity && $(OUT)_entity

# Per-op graph benchmark: optimized build (NO ASan / NO double-free-check — those
# skew timing). Emits per-op rdtsc cycle stats as JSON; CI compares base vs head.
BENCH_CFLAGS = -std=c11 -O2 -march=native -Wall -D_GNU_SOURCE -I.
bench: op_bench.c graph.c stringtable.c memoryfile.c
	$(CC) $(BENCH_CFLAGS) $^ -lm -o $(OUT)_bench && $(OUT)_bench

clean:
	rm -f $(OUT)_*
