CC      ?= gcc
CFLAGS  ?= -std=c11 -O2 -Wall -Wextra -D_GNU_SOURCE -march=native
ASAN     = -std=c11 -O1 -g -fsanitize=address,undefined -D_GNU_SOURCE
SUB      = substrate.c

.PHONY: all clean test
all: bench_radix bench_cart

bench_radix: $(SUB) mf_radix.c bench.c substrate.h mf_radix.h
	$(CC) $(CFLAGS) -DALLOC_RADIX -o $@ $(SUB) mf_radix.c bench.c

bench_cart: $(SUB) mf_cart.c bench.c substrate.h mf_cart.h
	$(CC) $(CFLAGS) -DALLOC_CART -o $@ $(SUB) mf_cart.c bench.c

test:
	$(CC) $(ASAN) -o test_coalesce $(SUB) mf_radix.c test_coalesce.c
	$(CC) $(ASAN) -o test_cart     $(SUB) mf_cart.c   test_cart.c
	./test_coalesce
	./test_cart

clean:
	rm -f bench_radix bench_cart test_coalesce test_cart
