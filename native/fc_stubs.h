/*
 * Frama-C analysis-only stubs: declarations Frama-C's libc model lacks
 * (GNU/Linux specifics). Force-included via -cpp-extra-args="-include fc_stubs.h".
 * NEVER compiled into the real build.
 */
#ifndef FC_STUBS_H
#define FC_STUBS_H
#include <stddef.h>

#ifndef MREMAP_MAYMOVE
#define MREMAP_MAYMOVE 1
#endif

extern void *mremap(void *old_address, size_t old_size, size_t new_size, int flags, ...);

#endif /* FC_STUBS_H */
