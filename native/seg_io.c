/*
 * seg_io.c — the two seg_io implementations.
 *
 * posix: fd + PROT_READ MAP_SHARED mapping. Writes via pwrite loop, barrier
 * via fdatasync, growth via ftruncate + remap. Page-cache coherence makes
 * pwrites visible through the RO mapping; there is no msync anywhere.
 *
 * sim: malloc'd disk image + append-only event log (WRITE/SYNC/EXTEND).
 * seg_io_sim_replay_prefix(k) materializes the disk as of any event prefix —
 * every legal crash state of an in-order device (the fdatasync-permutation
 * adversary is layered in the harness, not here).
 */
#include "segstore.h"

#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

/* ================= posix ================= */

typedef struct {
    seg_io_t vt;
    int      fd;
    u8      *map;      /* PROT_READ MAP_SHARED; NULL if file empty */
    u64      map_size;
} io_posix_t;

static int posix_write(seg_io_t *io, const void *buf, u64 len, u64 off) {
    io_posix_t *p = (io_posix_t *)io;
    const u8 *b = (const u8 *)buf;
    while (len > 0) {
        ssize_t n = pwrite(p->fd, b, len, (off_t)off);
        if (n < 0) { if (errno == EINTR) continue; return 0; }
        b += n; len -= (u64)n; off += (u64)n;
    }
    return 1;
}

static int posix_sync(seg_io_t *io) {
    io_posix_t *p = (io_posix_t *)io;
    return fdatasync(p->fd) == 0;
}

static int posix_remap(io_posix_t *p, u64 size) {
    if (p->map) { munmap(p->map, p->map_size); p->map = NULL; p->map_size = 0; }
    if (size == 0) return 1;
    void *m = mmap(NULL, size, PROT_READ, MAP_SHARED, p->fd, 0);
    if (m == MAP_FAILED) return 0;
    p->map = (u8 *)m; p->map_size = size;
    return 1;
}

static int posix_extend(seg_io_t *io, u64 new_size) {
    io_posix_t *p = (io_posix_t *)io;
    if (new_size <= p->map_size) return 1;
    if (ftruncate(p->fd, (off_t)new_size) != 0) return 0;
    return posix_remap(p, new_size);
}

static const u8 *posix_read_base(seg_io_t *io, u64 *size_out) {
    io_posix_t *p = (io_posix_t *)io;
    if (size_out) *size_out = p->map_size;
    return p->map;
}

static void posix_close(seg_io_t *io) {
    io_posix_t *p = (io_posix_t *)io;
    if (p->map) munmap(p->map, p->map_size);
    close(p->fd);
    free(p);
}

seg_io_t *seg_io_posix_open(const char *path, int create) {
    int flags = O_RDWR | (create ? O_CREAT : 0);
    int fd = open(path, flags, 0600);
    if (fd < 0) return NULL;
    struct stat st;
    if (fstat(fd, &st) != 0) { close(fd); return NULL; }
    io_posix_t *p = (io_posix_t *)calloc(1, sizeof *p);
    if (!p) { close(fd); return NULL; }
    p->vt.write     = posix_write;
    p->vt.sync      = posix_sync;
    p->vt.extend    = posix_extend;
    p->vt.read_base = posix_read_base;
    p->vt.close     = posix_close;
    p->fd = fd;
    if (st.st_size > 0 && !posix_remap(p, (u64)st.st_size)) {
        close(fd); free(p); return NULL;
    }
    return &p->vt;
}

/* ================= sim ================= */

typedef enum { EV_WRITE = 1, EV_SYNC = 2, EV_EXTEND = 3 } ev_kind_t;

typedef struct {
    ev_kind_t kind;
    u64       off, len;      /* WRITE: range; EXTEND: len = new size */
    u8       *bytes;         /* WRITE payload (owned) */
} sim_ev_t;

typedef struct {
    seg_io_t  vt;
    u8       *disk;          /* current image (all events applied) */
    u64       size;
    sim_ev_t *ev;
    u32       nev, evcap;
} io_sim_t;

static sim_ev_t *sim_push(io_sim_t *s, ev_kind_t k) {
    if (s->nev == s->evcap) {
        u32 nc = s->evcap ? s->evcap * 2 : 256;
        sim_ev_t *ne = (sim_ev_t *)realloc(s->ev, (size_t)nc * sizeof *ne);
        if (!ne) return NULL;
        s->ev = ne; s->evcap = nc;
    }
    sim_ev_t *e = &s->ev[s->nev++];
    memset(e, 0, sizeof *e);
    e->kind = k;
    return e;
}

static int sim_write(seg_io_t *io, const void *buf, u64 len, u64 off) {
    io_sim_t *s = (io_sim_t *)io;
    if (off + len > s->size) return 0;          /* write past EOF = bug */
    sim_ev_t *e = sim_push(s, EV_WRITE);
    if (!e) return 0;
    e->off = off; e->len = len;
    e->bytes = (u8 *)malloc(len);
    if (!e->bytes) { s->nev--; return 0; }
    memcpy(e->bytes, buf, len);
    memcpy(s->disk + off, buf, len);
    return 1;
}

static int sim_sync(seg_io_t *io) {
    io_sim_t *s = (io_sim_t *)io;
    return sim_push(s, EV_SYNC) != NULL;
}

static int sim_extend(seg_io_t *io, u64 new_size) {
    io_sim_t *s = (io_sim_t *)io;
    if (new_size <= s->size) return 1;
    u8 *nd = (u8 *)realloc(s->disk, new_size);
    if (!nd) return 0;
    memset(nd + s->size, 0, new_size - s->size);
    sim_ev_t *e = sim_push(s, EV_EXTEND);
    if (!e) { s->disk = nd; return 0; }
    e->len = new_size;
    s->disk = nd; s->size = new_size;
    return 1;
}

static const u8 *sim_read_base(seg_io_t *io, u64 *size_out) {
    io_sim_t *s = (io_sim_t *)io;
    if (size_out) *size_out = s->size;
    return s->disk;
}

static void sim_close(seg_io_t *io) {
    io_sim_t *s = (io_sim_t *)io;
    for (u32 i = 0; i < s->nev; i++) free(s->ev[i].bytes);
    free(s->ev); free(s->disk); free(s);
}

seg_io_t *seg_io_sim_open(void) {
    io_sim_t *s = (io_sim_t *)calloc(1, sizeof *s);
    if (!s) return NULL;
    s->vt.write     = sim_write;
    s->vt.sync      = sim_sync;
    s->vt.extend    = sim_extend;
    s->vt.read_base = sim_read_base;
    s->vt.close     = sim_close;
    return &s->vt;
}

u32 seg_io_sim_event_count(const seg_io_t *io) {
    return ((const io_sim_t *)io)->nev;
}

u8 *seg_io_sim_replay_prefix(const seg_io_t *io, u32 k, u64 *size_out) {
    const io_sim_t *s = (const io_sim_t *)io;
    if (k > s->nev) k = s->nev;
    u8 *disk = NULL; u64 size = 0;
    for (u32 i = 0; i < k; i++) {
        const sim_ev_t *e = &s->ev[i];
        switch (e->kind) {
        case EV_EXTEND: {
            u8 *nd = (u8 *)realloc(disk, e->len);
            if (!nd && e->len) { free(disk); return NULL; }
            memset(nd + size, 0, e->len - size);
            disk = nd; size = e->len;
            break;
        }
        case EV_WRITE:
            if (e->off + e->len <= size)
                memcpy(disk + e->off, e->bytes, e->len);
            break;
        case EV_SYNC:
            break;
        }
    }
    if (size_out) *size_out = size;
    return disk;
}

u32 seg_io_sim_last_sync_before(const seg_io_t *io, u32 k) {
    const io_sim_t *s = (const io_sim_t *)io;
    if (k > s->nev) k = s->nev;
    u32 last = 0;
    for (u32 i = 0; i < k; i++)
        if (s->ev[i].kind == EV_SYNC) last = i + 1;
    return last;
}
