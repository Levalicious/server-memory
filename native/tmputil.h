/*
 * tmputil.h — per-process scratch paths for the v3 store's TEST + BENCH harnesses
 * (never compiled into the published addon). Concurrent runs — e.g. a git-worktree
 * agent swarm building/testing/benching in parallel — must not collide on fixed
 * /tmp files. Each path honors $TMPDIR (fallback /tmp) and is disambiguated by PID.
 */
#ifndef TMPUTIL_H
#define TMPUTIL_H
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

static inline char *mf_tmppath(char *buf, size_t cap, const char *name) {
    const char *td = getenv("TMPDIR");
    if (!td || !*td) td = "/tmp";
    snprintf(buf, cap, "%s/%s.%ld", td, name, (long)getpid());
    return buf;
}

/* Drop-in replacement for a literal scratch path: MF_TMP("foo.dat"). Each call is
 * backed by its own block-scope compound literal, so multiple uses in one scope
 * stay distinct (valid as long as the path is used within that scope). */
#define MF_TMP(name) mf_tmppath((char[256]){0}, 256, (name))

#endif /* TMPUTIL_H */
