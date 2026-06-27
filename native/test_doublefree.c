/*
 * Negative test for the test-only double-free detector. A deliberate double-free
 * MUST trip MEMFILE_DOUBLE_FREE_CHECK and abort. Driven by `make verify-detector`,
 * which asserts the detector message is printed. Reaching `return 0` = the
 * detector FAILED to fire.
 */
#include "memoryfile.h"
#include <unistd.h>

int main(void) {
    const char *path = "/tmp/mf_doublefree_scratch.dat";
    unlink(path);
    memfile_t *mf = memfile_open(path, 65536);
    if (!mf) return 2;
    u64 a = memfile_alloc(mf, 100);
    memfile_free(mf, a, 100);
    memfile_free(mf, a, 100);   /* detector must abort here */
    unlink(path);
    return 0;
}
