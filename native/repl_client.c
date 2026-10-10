/*
 * repl_client.c — the anti-entropy wire client (seam 5b-i-β). See
 * repl_client.h. Frames are daemon_proto.h; the cursor helpers mirror the
 * daemon's (both are the same little-endian contract, kept local so neither
 * side drags the other's internals in).
 */
#include "repl_client.h"
#include "daemon_proto.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

/* ---- cursor helpers (same contract as the daemon's) ---- */

typedef struct { u8 *buf; u32 len, cap, off; int err; } cw_t;

static int cw_grow(cw_t *w, u32 need) {
    if (w->len + need <= w->cap) return 1;
    u32 nc = w->cap ? w->cap : 256;
    while (nc < w->len + need) nc *= 2;
    u8 *nb = (u8 *)realloc(w->buf, nc);
    if (!nb) { w->err = 1; return 0; }
    w->buf = nb; w->cap = nc;
    return 1;
}
static void cw_u8(cw_t *w, u8 v) { if (cw_grow(w, 1)) w->buf[w->len++] = v; }
static void cw_u32(cw_t *w, u32 v) {
    if (cw_grow(w, 4)) {
        u8 *p = w->buf + w->len;
        p[0] = (u8)v; p[1] = (u8)(v >> 8); p[2] = (u8)(v >> 16); p[3] = (u8)(v >> 24);
        w->len += 4;
    }
}
static void cw_u64(cw_t *w, u64 v) { cw_u32(w, (u32)v); cw_u32(w, (u32)(v >> 32)); }
static void cw_str(cw_t *w, const u8 *s, u16 l) {
    if (!cw_grow(w, 2u + l)) return;
    w->buf[w->len++] = (u8)l; w->buf[w->len++] = (u8)(l >> 8);
    memcpy(w->buf + w->len, s, l);
    w->len += l;
}
static void cw_bytes(cw_t *w, const u8 *s, u32 n) {
    if (cw_grow(w, n)) { memcpy(w->buf + w->len, s, n); w->len += n; }
}

typedef struct { const u8 *p, *end; int err; } cr_t;
static u8 cr_u8(cr_t *r) { if (r->p >= r->end) { r->err = 1; return 0; } return *r->p++; }
static u32 cr_u32(cr_t *r) {
    if (r->p + 4 > r->end) { r->err = 1; return 0; }
    u32 v = (u32)r->p[0] | ((u32)r->p[1] << 8) | ((u32)r->p[2] << 16) | ((u32)r->p[3] << 24);
    r->p += 4;
    return v;
}
static const u8 *cr_str(cr_t *r, u16 *len) {
    if (r->p + 2 > r->end) { r->err = 1; return NULL; }
    u16 l = (u16)(r->p[0] | (r->p[1] << 8));
    r->p += 2;
    if (r->p + l > r->end) { r->err = 1; return NULL; }
    const u8 *s = r->p;
    r->p += l;
    *len = l;
    return s;
}

/* ---- connection ---- */

struct g4_wire_peer {
    int fd;
    u32 reqid;
    u32 want_gen[2];
    int begun[2];
    repl_peer_t iface;
};

static int io_all(int fd, u8 *p, u32 n, int writing) {
    while (n) {
        ssize_t s = writing ? write(fd, p, n) : read(fd, p, n);
        if (s <= 0) {
            if (s < 0 && errno == EINTR) continue;
            return 0;
        }
        p += s;
        n -= (u32)s;
    }
    return 1;
}

/* one request/response; on ST_OK the body is malloc'd into *body (*blen). */
static int wp_rt(g4_wire_peer_t *p, u8 op, const cw_t *pl, u8 **body, u32 *blen) {
    u8 hdr[9];
    u32 len = 5 + pl->len;
    if (len > KBD_MAX_FRAME || pl->err) return 0;
    hdr[0] = (u8)len; hdr[1] = (u8)(len >> 8); hdr[2] = (u8)(len >> 16); hdr[3] = (u8)(len >> 24);
    u32 rid = ++p->reqid;
    hdr[4] = (u8)rid; hdr[5] = (u8)(rid >> 8); hdr[6] = (u8)(rid >> 16); hdr[7] = (u8)(rid >> 24);
    hdr[8] = op;
    if (!io_all(p->fd, hdr, 9, 1)) return 0;
    if (pl->len && !io_all(p->fd, pl->buf, pl->len, 1)) return 0;

    if (!io_all(p->fd, hdr, 9, 0)) return 0;
    u32 rlen = (u32)hdr[0] | ((u32)hdr[1] << 8) | ((u32)hdr[2] << 16) | ((u32)hdr[3] << 24);
    u32 rrid = (u32)hdr[4] | ((u32)hdr[5] << 8) | ((u32)hdr[6] << 16) | ((u32)hdr[7] << 24);
    u8 status = hdr[8];
    if (rlen < 5 || rlen > KBD_MAX_FRAME || rrid != rid) return 0;
    u32 rbl = rlen - 5;
    u8 *b = NULL;
    if (rbl) {
        b = (u8 *)malloc(rbl);
        if (!b) return 0;
        if (!io_all(p->fd, b, rbl, 0)) { free(b); return 0; }
    }
    *body = b;
    *blen = rbl;
    return status == ST_OK ? 1 : 2;
}

g4_wire_peer_t *g4_wire_peer_open(const char *host, unsigned short port,
                                  const u8 *token, u16 tlen) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return NULL;
    struct sockaddr_in a;
    memset(&a, 0, sizeof a);
    a.sin_family = AF_INET;
    a.sin_port = htons(port);
    if (inet_pton(AF_INET, host, &a.sin_addr) != 1 ||
        connect(fd, (struct sockaddr *)&a, sizeof a) != 0) {
        close(fd);
        return NULL;
    }
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
    struct timeval tv;
    tv.tv_sec = G4_REPL_IO_TIMEOUT_MS / 1000;
    tv.tv_usec = (G4_REPL_IO_TIMEOUT_MS % 1000) * 1000;
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof tv);

    g4_wire_peer_t *p = (g4_wire_peer_t *)calloc(1, sizeof *p);
    if (!p) { close(fd); return NULL; }
    p->fd = fd;
    cw_t pl = {0};
    cw_str(&pl, token, tlen);
    u8 *body = NULL;
    u32 blen = 0;
    int r = wp_rt(p, OP_AUTH, &pl, &body, &blen);
    free(pl.buf);
    free(body);
    if (r != 1) {          /* connect failure, bad token, or transport error */
        close(fd);
        free(p);
        return NULL;
    }
    return p;
}

void g4_wire_peer_close(g4_wire_peer_t *p) {
    if (!p) return;
    if (p->fd >= 0) close(p->fd);
    free(p);
}

/* ---- the repl_peer_t vtable ---- */

static int wp_begin(void *ctx, u8 family, u32 *nsym) {
    g4_wire_peer_t *p = (g4_wire_peer_t *)ctx;
    if (family > 1) return 0;
    cw_t pl = {0};
    cw_u8(&pl, family);
    u8 *body = NULL;
    u32 blen = 0;
    int r = wp_rt(p, OP_RE_BEGIN, &pl, &body, &blen);
    free(pl.buf);
    if (r != 1) { free(body); return 0; }
    cr_t cr = { body, body + blen, 0 };
    u32 n = cr_u32(&cr), gen = cr_u32(&cr);
    free(body);
    if (cr.err) return 0;
    p->want_gen[family] = gen;
    p->begun[family] = 1;
    if (nsym) *nsym = n;
    return 1;
}

static int wp_cells(void *ctx, u8 family, u32 from_idx, u32 max, u8 *out, u32 *nout) {
    g4_wire_peer_t *p = (g4_wire_peer_t *)ctx;
    if (family > 1 || !p->begun[family]) return 0;
    cw_t pl = {0};
    cw_u8(&pl, family);
    cw_u32(&pl, from_idx);
    cw_u32(&pl, max);
    u8 *body = NULL;
    u32 blen = 0;
    int r = wp_rt(p, OP_RE_CELLS, &pl, &body, &blen);
    free(pl.buf);
    if (r != 1) { free(body); return 0; }
    cr_t cr = { body, body + blen, 0 };
    u32 gen = cr_u32(&cr), n = cr_u32(&cr);
    if (cr.err || gen != p->want_gen[family] || n > max ||
        (blen - 8) != (u64)n * 44u) {
        free(body);
        return 0;                     /* replaced snapshot: fail the round */
    }
    memcpy(out, cr.p, (size_t)n * 44u);
    free(body);
    *nout = n;
    return 1;
}

static int wp_end(void *ctx, u8 family) {
    g4_wire_peer_t *p = (g4_wire_peer_t *)ctx;
    if (family > 1) return 0;
    cw_t pl = {0};
    cw_u8(&pl, family);
    u8 *body = NULL;
    u32 blen = 0;
    int r = wp_rt(p, OP_RE_END, &pl, &body, &blen);
    free(pl.buf);
    free(body);
    p->begun[family] = 0;
    return r == 1;
}

static int wp_relname(void *ctx, u64 relhash, u8 *out, u16 *outlen) {
    g4_wire_peer_t *p = (g4_wire_peer_t *)ctx;
    cw_t pl = {0};
    cw_u32(&pl, 1);
    cw_u64(&pl, relhash);
    u8 *body = NULL;
    u32 blen = 0;
    int r = wp_rt(p, OP_RE_RELNAME, &pl, &body, &blen);
    free(pl.buf);
    if (r != 1) { free(body); return 0; }
    cr_t cr = { body, body + blen, 0 };
    u8 found = cr_u8(&cr);
    u16 l = 0;
    const u8 *s = NULL;
    if (found == 1 && !cr.err) s = cr_str(&cr, &l);
    int ok = found == 1 && s && l <= G4_REPL_STR_CAP && !cr.err;
    if (ok) {
        memcpy(out, s, l);
        *outlen = l;
    }
    free(body);
    return ok;
}

static int wp_vrow(void *ctx, u32 node, u8 *out, u32 *outlen) {
    g4_wire_peer_t *p = (g4_wire_peer_t *)ctx;
    cw_t pl = {0};
    cw_u32(&pl, 1);
    cw_u32(&pl, node);
    u8 *body = NULL;
    u32 blen = 0;
    int r = wp_rt(p, OP_RE_VROW, &pl, &body, &blen);
    free(pl.buf);
    if (r != 1) { free(body); return 0; }
    cr_t cr = { body, body + blen, 0 };
    u8 found = cr_u8(&cr);
    u32 l = 0;
    int ok = 0;
    if (found == 1 && !cr.err) {
        l = cr_u32(&cr);
        if (!cr.err && cr.p + l <= cr.end && l <= G4_REPL_ROW_CAP) {
            memcpy(out, cr.p, l);
            *outlen = l;
            ok = 1;
        }
    }
    free(body);
    return ok;
}

static int wp_vrow_set(void *ctx, u32 node, const u8 *row, u32 rowlen, u8 *status) {
    g4_wire_peer_t *p = (g4_wire_peer_t *)ctx;
    cw_t pl = {0};
    cw_u32(&pl, 1);
    cw_u32(&pl, node);
    cw_u32(&pl, rowlen);
    cw_bytes(&pl, row, rowlen);
    u8 *body = NULL;
    u32 blen = 0;
    int r = wp_rt(p, OP_RE_VROW_SET, &pl, &body, &blen);
    free(pl.buf);
    if (r != 1) { free(body); return 0; }
    cr_t cr = { body, body + blen, 0 };
    u8 st = cr_u8(&cr);
    free(body);
    if (cr.err) return 0;
    *status = st;
    return 1;
}

static int wp_edge_pull(void *ctx, const u8 *sym, const u8 *rt, u16 rl, u8 *status) {
    g4_wire_peer_t *p = (g4_wire_peer_t *)ctx;
    cw_t pl = {0};
    cw_u32(&pl, 1);
    cw_bytes(&pl, sym, G4_SYM_LEN);
    cw_str(&pl, rt, rl);
    u8 *body = NULL;
    u32 blen = 0;
    int r = wp_rt(p, OP_RE_EDGE_PULL, &pl, &body, &blen);
    free(pl.buf);
    if (r != 1) { free(body); return 0; }
    cr_t cr = { body, body + blen, 0 };
    u8 st = cr_u8(&cr);
    free(body);
    if (cr.err) return 0;
    *status = st;
    return 1;
}

static int wp_edge_del(void *ctx, const u8 *sym, u8 *status) {
    g4_wire_peer_t *p = (g4_wire_peer_t *)ctx;
    cw_t pl = {0};
    cw_u32(&pl, 1);
    cw_bytes(&pl, sym, G4_SYM_LEN);
    u8 *body = NULL;
    u32 blen = 0;
    int r = wp_rt(p, OP_RE_EDGE_DEL, &pl, &body, &blen);
    free(pl.buf);
    if (r != 1) { free(body); return 0; }
    cr_t cr = { body, body + blen, 0 };
    u8 st = cr_u8(&cr);
    free(body);
    if (cr.err) return 0;
    *status = st;
    return 1;
}

static int wp_name_get(void *ctx, const u8 *name, u16 nl, u32 *node, u32 *gen) {
    g4_wire_peer_t *p = (g4_wire_peer_t *)ctx;
    cw_t pl = {0};
    cw_u32(&pl, 1);
    cw_str(&pl, name, nl);
    u8 *body = NULL;
    u32 blen = 0;
    int r = wp_rt(p, OP_RE_NAME, &pl, &body, &blen);
    free(pl.buf);
    if (r != 1) { free(body); return 0; }
    cr_t cr = { body, body + blen, 0 };
    u8 found = cr_u8(&cr);
    if (found == 1) { *node = cr_u32(&cr); *gen = cr_u32(&cr); }
    int ok = found == 1 && !cr.err;
    free(body);
    return ok;
}

static int wp_name_set(void *ctx, const u8 *name, u16 nl, u32 node, u32 gen, u8 *status) {
    g4_wire_peer_t *p = (g4_wire_peer_t *)ctx;
    cw_t pl = {0};
    cw_u32(&pl, 1);
    cw_str(&pl, name, nl);
    cw_u32(&pl, node);
    cw_u32(&pl, gen);
    u8 *body = NULL;
    u32 blen = 0;
    int r = wp_rt(p, OP_RE_NAME_SET, &pl, &body, &blen);
    free(pl.buf);
    if (r != 1) { free(body); return 0; }
    cr_t cr = { body, body + blen, 0 };
    u8 st = cr_u8(&cr);
    free(body);
    if (cr.err) return 0;
    *status = st;
    return 1;
}

static int wp_entity_get(void *ctx, u32 node, u8 *name, u16 *nl, u32 *gen,
                         u8 *blob, u32 *blen) {
    g4_wire_peer_t *p = (g4_wire_peer_t *)ctx;
    cw_t pl = {0};
    cw_u32(&pl, 1);
    cw_u32(&pl, node);
    u8 *body = NULL;
    u32 rbl = 0;
    int r = wp_rt(p, OP_RE_ENTITY, &pl, &body, &rbl);
    free(pl.buf);
    if (r != 1) { free(body); return 0; }
    cr_t cr = { body, body + rbl, 0 };
    u8 found = cr_u8(&cr);
    int ok = 0;
    if (found == 1 && !cr.err) {
        const u8 *nm = cr_str(&cr, nl);
        u32 g = cr_u32(&cr), bl = cr_u32(&cr);
        if (nm && !cr.err && *nl <= G4_REPL_STR_CAP && bl <= G4_REPL_ROW_CAP &&
            cr.p + bl <= cr.end) {
            memcpy(name, nm, *nl);
            memcpy(blob, cr.p, bl);
            *gen = g;
            *blen = bl;
            ok = 1;
        }
    }
    free(body);
    return ok;
}

static int wp_entity_set(void *ctx, u32 node, const u8 *name, u16 nl, u32 gen,
                         const u8 *blob, u32 blen, u8 *status) {
    g4_wire_peer_t *p = (g4_wire_peer_t *)ctx;
    cw_t pl = {0};
    cw_u32(&pl, 1);
    cw_u32(&pl, node);
    cw_str(&pl, name, nl);
    cw_u32(&pl, gen);
    cw_u32(&pl, blen);
    cw_bytes(&pl, blob, blen);
    u8 *body = NULL;
    u32 rbl = 0;
    int r = wp_rt(p, OP_RE_ENTITY_SET, &pl, &body, &rbl);
    free(pl.buf);
    if (r != 1) { free(body); return 0; }
    cr_t cr = { body, body + rbl, 0 };
    u8 st = cr_u8(&cr);
    free(body);
    if (cr.err) return 0;
    *status = st;
    return 1;
}

repl_peer_t *g4_wire_peer_iface(g4_wire_peer_t *p) {
    p->iface.ctx = p;
    p->iface.begin = wp_begin;
    p->iface.cells = wp_cells;
    p->iface.end = wp_end;
    p->iface.relname = wp_relname;
    p->iface.vrow = wp_vrow;
    p->iface.vrow_set = wp_vrow_set;
    p->iface.edge_pull = wp_edge_pull;
    p->iface.edge_del = wp_edge_del;
    p->iface.name_get = wp_name_get;
    p->iface.name_set = wp_name_set;
    p->iface.entity_get = wp_entity_get;
    p->iface.entity_set = wp_entity_set;
    return &p->iface;
}
