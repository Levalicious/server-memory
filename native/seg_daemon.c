/*
 * seg_daemon.c — kbd4: the owner daemon (spec r3 §1, §6;
 * Decision_ExternalizeViaCDaemon).
 *
 * Single-threaded poll() event loop — the loop IS the single writer; there
 * is no lock anywhere. Clients speak the daemon_proto.h frames over TCP
 * with a bearer-token handshake.
 *
 * Write model: one write request = one mstore txn = one group commit
 * (batch-shaped API end-to-end). Reads run against the committed state.
 *
 * Counter relaxation (spec §3): walker visits accumulate in memory
 * (pending map) and flush INSIDE the next write txn (piggyback) and at
 * shutdown — a crash loses rank statistics, never graph truth.
 *
 * kbd4 <store_dir> <port> <token_file>
 *   store_dir/{manifest.kb, graph.kb, strings.kb} created if absent.
 *   port 0 = ephemeral; the bound port is printed as "PORT <n>\n" on
 *   stdout (test harness + supervisor handshake).
 */
#include "segstore.h"
#include "daemon_proto.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

/* ---------- little-endian cursor r/w ---------- */

typedef struct { const u8 *p, *end; int err; } rd_t;
typedef struct { u8 *buf; u32 len, cap; int err; } wr_t;

static u32 r32(rd_t *r) { if (r->p + 4 > r->end) { r->err = 1; return 0; }
    u32 v = (u32)r->p[0]|((u32)r->p[1]<<8)|((u32)r->p[2]<<16)|((u32)r->p[3]<<24); r->p += 4; return v; }
static u64 r64(rd_t *r) { u64 lo = r32(r), hi = r32(r); return lo | (hi << 32); }
static u8  r8(rd_t *r)  { if (r->p >= r->end) { r->err = 1; return 0; } return *r->p++; }
static const u8 *rstr(rd_t *r, u16 *len) {
    if (r->p + 2 > r->end) { r->err = 1; return NULL; }
    u16 l = (u16)(r->p[0] | (r->p[1] << 8)); r->p += 2;
    if (r->p + l > r->end) { r->err = 1; return NULL; }
    const u8 *s = r->p; r->p += l; *len = l; return s;
}

static int wgrow(wr_t *w, u32 need) {
    if (w->len + need <= w->cap) return 1;
    u32 nc = w->cap ? w->cap : 256;
    while (nc < w->len + need) nc *= 2;
    u8 *nb = (u8 *)realloc(w->buf, nc);
    if (!nb) { w->err = 1; return 0; }
    w->buf = nb; w->cap = nc; return 1;
}
static void w8(wr_t *w, u8 v)   { if (wgrow(w, 1)) w->buf[w->len++] = v; }
static void w32(wr_t *w, u32 v) { if (wgrow(w, 4)) { u8 *p = w->buf + w->len;
    p[0]=(u8)v; p[1]=(u8)(v>>8); p[2]=(u8)(v>>16); p[3]=(u8)(v>>24); w->len += 4; } }
static void w64(wr_t *w, u64 v) { w32(w, (u32)v); w32(w, (u32)(v >> 32)); }
static void wstr(wr_t *w, const u8 *s, u16 l) {
    if (wgrow(w, 2u + l)) {
        w->buf[w->len++] = (u8)l; w->buf[w->len++] = (u8)(l >> 8);
        memcpy(w->buf + w->len, s, l); w->len += l;
    }
}

/* ---------- daemon state ---------- */

typedef struct { u32 *eids; u64 *walks; u32 n, cap; } pend_walk_t;

typedef struct {
    mstore_t  *ms;
    graph4_t  *g;
    char       token[256]; u16 token_len;
    pend_walk_t pw;                   /* pending walker-visit counts */
    int        dirty_counters;
} kbd_t;

static void pw_add(kbd_t *k, u32 eid) {
    for (u32 i = 0; i < k->pw.n; i++)
        if (k->pw.eids[i] == eid) { k->pw.walks[i]++; k->dirty_counters = 1; return; }
    if (k->pw.n == k->pw.cap) {
        u32 nc = k->pw.cap ? k->pw.cap * 2 : 64;
        u32 *ne = (u32 *)realloc(k->pw.eids, (size_t)nc * 4);
        u64 *nw = (u64 *)realloc(k->pw.walks, (size_t)nc * 8);
        if (!ne || !nw) { free(ne); return; }
        k->pw.eids = ne; k->pw.walks = nw; k->pw.cap = nc;
    }
    k->pw.eids[k->pw.n] = eid; k->pw.walks[k->pw.n] = 1; k->pw.n++;
    k->dirty_counters = 1;
}

/* flush pending walker visits inside an OPEN txn */
static void pw_flush_in_txn(kbd_t *k) {
    for (u32 i = 0; i < k->pw.n; i++)
        for (u64 c = 0; c < k->pw.walks[i]; c++)
            g4_inc_walker_visit(k->g, k->pw.eids[i]);
    k->pw.n = 0;
    k->dirty_counters = 0;
}

/* ---------- op handlers ---------- */

static void err_reply(wr_t *w, const char *msg) {
    w->len = 0;                        /* discard partial payload */
    wstr(w, (const u8 *)msg, (u16)strlen(msg));
}

static u32 lookup_or0(kbd_t *k, const u8 *nm, u16 nl) {
    return nm ? g4_lookup(k->g, nm, nl) : 0;
}

static void put_name(kbd_t *k, wr_t *w, u32 eid) {
    g4_entity_t e;
    if (!g4_read_entity(k->g, eid, &e)) { wstr(w, (const u8 *)"?", 1); return; }
    u16 l = 0;
    const u8 *b = g4_str(k->g, e.name_sid, &l);
    wstr(w, b ? b : (const u8 *)"?", b ? l : 1);
}

static u32 wire_dir(u8 d) {
    return d == 0 ? G4_DIR_FORWARD : d == 1 ? G4_DIR_BACKWARD : G4_DIR_ANY;
}

/* returns ST_*; response payload in w */
static int handle_op(kbd_t *k, u8 op, rd_t *r, wr_t *w) {
    switch (op) {
    case OP_PING:
        w32(w, KBD_PROTO_VER);
        return ST_OK;

    case OP_STATS:
        w32(w, g4_entity_count(k->g));
        w32(w, g4_relation_count(k->g));
        w64(w, mstore_txid(k->ms));
        return ST_OK;

    case OP_CREATE_ENTITIES: {
        u32 n = r32(r);
        if (r->err || n > 100000) { err_reply(w, "bad batch"); return ST_ERR; }
        if (!mstore_txn_begin(k->ms)) { err_reply(w, "txn"); return ST_ERR; }
        if (k->dirty_counters) pw_flush_in_txn(k);
        for (u32 i = 0; i < n; i++) {
            u16 nl, tl;
            const u8 *nm = rstr(r, &nl), *ty = rstr(r, &tl);
            u64 mt = r64(r);
            if (r->err) { mstore_txn_abort(k->ms); err_reply(w, "trunc"); return ST_ERR; }
            u32 eid = g4_create_entity(k->g, nm, nl, ty, tl, mt);
            w32(w, eid);
        }
        if (!mstore_txn_commit(k->ms)) { err_reply(w, "commit"); return ST_ERR; }
        return ST_OK;
    }
    case OP_DELETE_ENTITIES: {
        u32 n = r32(r);
        if (r->err || n > 100000) { err_reply(w, "bad batch"); return ST_ERR; }
        if (!mstore_txn_begin(k->ms)) { err_reply(w, "txn"); return ST_ERR; }
        if (k->dirty_counters) pw_flush_in_txn(k);
        for (u32 i = 0; i < n; i++) {
            u16 nl; const u8 *nm = rstr(r, &nl);
            if (r->err) { mstore_txn_abort(k->ms); err_reply(w, "trunc"); return ST_ERR; }
            u32 eid = lookup_or0(k, nm, nl);
            w8(w, eid ? (u8)g4_delete_entity(k->g, eid) : 0);
        }
        if (!mstore_txn_commit(k->ms)) { err_reply(w, "commit"); return ST_ERR; }
        return ST_OK;
    }
    case OP_CREATE_RELATIONS: case OP_DELETE_RELATIONS: {
        u32 n = r32(r);
        if (r->err || n > 100000) { err_reply(w, "bad batch"); return ST_ERR; }
        if (!mstore_txn_begin(k->ms)) { err_reply(w, "txn"); return ST_ERR; }
        if (k->dirty_counters) pw_flush_in_txn(k);
        for (u32 i = 0; i < n; i++) {
            u16 fl, tl, rl;
            const u8 *f = rstr(r, &fl), *t = rstr(r, &tl), *rt = rstr(r, &rl);
            u64 mt = (op == OP_CREATE_RELATIONS) ? r64(r) : 0;
            if (r->err) { mstore_txn_abort(k->ms); err_reply(w, "trunc"); return ST_ERR; }
            u32 fe = lookup_or0(k, f, fl), te = lookup_or0(k, t, tl);
            int ok = 0;
            if (fe && te)
                ok = (op == OP_CREATE_RELATIONS)
                   ? g4_create_relation(k->g, fe, te, rt, rl, mt)
                   : g4_delete_relation(k->g, fe, te, rt, rl);
            w8(w, (u8)ok);
        }
        if (!mstore_txn_commit(k->ms)) { err_reply(w, "commit"); return ST_ERR; }
        return ST_OK;
    }
    case OP_ADD_OBS: case OP_DEL_OBS: {
        u32 n = r32(r);
        if (r->err || n > 100000) { err_reply(w, "bad batch"); return ST_ERR; }
        if (!mstore_txn_begin(k->ms)) { err_reply(w, "txn"); return ST_ERR; }
        if (k->dirty_counters) pw_flush_in_txn(k);
        for (u32 i = 0; i < n; i++) {
            u16 nl, ol;
            const u8 *nm = rstr(r, &nl), *ob = rstr(r, &ol);
            u64 mt = r64(r);
            if (r->err) { mstore_txn_abort(k->ms); err_reply(w, "trunc"); return ST_ERR; }
            u32 eid = lookup_or0(k, nm, nl);
            int ok = 0;
            if (eid)
                ok = (op == OP_ADD_OBS)
                   ? g4_add_observation(k->g, eid, ob, ol, mt)
                   : g4_remove_observation(k->g, eid, ob, ol, mt);
            w8(w, (u8)ok);
        }
        if (!mstore_txn_commit(k->ms)) { err_reply(w, "commit"); return ST_ERR; }
        return ST_OK;
    }
    case OP_OPEN_NODES: {
        u32 n = r32(r);
        if (r->err || n > 100000) { err_reply(w, "bad batch"); return ST_ERR; }
        for (u32 i = 0; i < n; i++) {
            u16 nl; const u8 *nm = rstr(r, &nl);
            if (r->err) { err_reply(w, "trunc"); return ST_ERR; }
            u32 eid = lookup_or0(k, nm, nl);
            g4_entity_t e;
            if (!eid || !g4_read_entity(k->g, eid, &e)) { w8(w, 0); continue; }
            w8(w, 1);
            pw_add(k, eid);                          /* walker visit (relaxed) */
            u16 l = 0; const u8 *b;
            b = g4_str(k->g, e.name_sid, &l); wstr(w, b, l);
            b = g4_str(k->g, e.type_sid, &l); wstr(w, b, l);
            w64(w, e.mtime); w64(w, e.obs_mtime);
            w8(w, e.obs_count);
            if (e.obs_count >= 1) { b = g4_str(k->g, e.obs0_sid, &l); wstr(w, b, l); }
            if (e.obs_count >= 2) { b = g4_str(k->g, e.obs1_sid, &l); wstr(w, b, l); }
            u32 ec = g4_edge_count(k->g, eid);
            w32(w, ec);
            if (ec) {
                g4_edge_t *es = (g4_edge_t *)malloc((size_t)ec * sizeof *es);
                if (!es) { err_reply(w, "oom"); return ST_ERR; }
                g4_edges(k->g, eid, es, ec);
                for (u32 x = 0; x < ec; x++) {
                    b = g4_str(k->g, es[x].rel_sid, &l); wstr(w, b, l);
                    w8(w, (u8)es[x].direction);
                    put_name(k, w, es[x].target_eid);
                    w64(w, es[x].mtime);
                }
                free(es);
            }
        }
        return ST_OK;
    }
    case OP_NEIGHBORS: {
        u16 nl; const u8 *nm = rstr(r, &nl);
        u32 depth = r32(r); u8 dir = r8(r); u32 max = r32(r);
        if (r->err || max > 100000) { err_reply(w, "bad req"); return ST_ERR; }
        u32 eid = lookup_or0(k, nm, nl);
        if (!eid) { err_reply(w, "no such entity"); return ST_ERR; }
        u32 *out = (u32 *)malloc((size_t)(max ? max : 1) * 4);
        if (!out) { err_reply(w, "oom"); return ST_ERR; }
        /* PUBLIC 0-indexed -> C hop count: THE +1 boundary (spec §6.3) */
        u32 total = g4_neighbors(k->g, eid, depth + 1, wire_dir(dir), out, max);
        u32 kept = total < max ? total : max;
        w32(w, total);
        for (u32 i = 0; i < kept; i++) { put_name(k, w, out[i]); pw_add(k, out[i]); }
        free(out);
        return ST_OK;
    }
    case OP_FIND_PATH: {
        u16 fl, tl;
        const u8 *f = rstr(r, &fl), *t = rstr(r, &tl);
        u32 maxd = r32(r); u8 dir = r8(r);
        if (r->err || maxd > 64) { err_reply(w, "bad req"); return ST_ERR; }
        u32 fe = lookup_or0(k, f, fl), te = lookup_or0(k, t, tl);
        if (!fe || !te) { err_reply(w, "no such entity"); return ST_ERR; }
        u32 path[128];
        u32 n = g4_find_path(k->g, fe, te, maxd, wire_dir(dir), path, 128);
        u32 kept = n < 128 ? n : 128;
        w32(w, n);
        for (u32 i = 0; i < kept; i++) put_name(k, w, path[i]);
        return ST_OK;
    }
    case OP_SEARCH: case OP_BY_TYPE: case OP_ORPHANED: {
        u32 total = 0, max = 0;
        u32 *out = NULL;
        if (op == OP_SEARCH) {
            u16 pl; const u8 *pat = rstr(r, &pl);
            max = r32(r);
            if (r->err || max > 100000) { err_reply(w, "bad req"); return ST_ERR; }
            char *pz = (char *)malloc((size_t)pl + 1);
            if (!pz) { err_reply(w, "oom"); return ST_ERR; }
            memcpy(pz, pat, pl); pz[pl] = 0;
            if (!g4_regex_valid(pz)) { free(pz); err_reply(w, "invalid pattern"); return ST_ERR; }
            out = (u32 *)malloc((size_t)(max ? max : 1) * 4);
            if (!out) { free(pz); err_reply(w, "oom"); return ST_ERR; }
            total = g4_search(k->g, pz, out, max);
            free(pz);
        } else if (op == OP_BY_TYPE) {
            u16 tl; const u8 *ty = rstr(r, &tl);
            max = r32(r);
            if (r->err || max > 100000) { err_reply(w, "bad req"); return ST_ERR; }
            out = (u32 *)malloc((size_t)(max ? max : 1) * 4);
            if (!out) { err_reply(w, "oom"); return ST_ERR; }
            total = g4_entities_by_type(k->g, ty, tl, out, max);
        } else {
            max = r32(r);
            if (r->err || max > 100000) { err_reply(w, "bad req"); return ST_ERR; }
            out = (u32 *)malloc((size_t)(max ? max : 1) * 4);
            if (!out) { err_reply(w, "oom"); return ST_ERR; }
            total = g4_orphaned(k->g, out, max);
        }
        u32 kept = total < max ? total : max;
        w32(w, total);
        for (u32 i = 0; i < kept; i++) put_name(k, w, out[i]);
        free(out);
        return ST_OK;
    }
    case OP_ENTITY_TYPES: case OP_RELATION_TYPES: {
        u32 max = r32(r);
        if (r->err || max > 100000) { err_reply(w, "bad req"); return ST_ERR; }
        u32 *sids = (u32 *)malloc((size_t)(max ? max : 1) * 4);
        if (!sids) { err_reply(w, "oom"); return ST_ERR; }
        u32 total = (op == OP_ENTITY_TYPES)
                  ? g4_entity_types(k->g, sids, max)
                  : g4_relation_types(k->g, sids, max);
        u32 kept = total < max ? total : max;
        w32(w, total);
        for (u32 i = 0; i < kept; i++) {
            u16 l = 0;
            const u8 *b = g4_str(k->g, sids[i], &l);
            wstr(w, b ? b : (const u8 *)"?", b ? l : 1);
        }
        free(sids);
        return ST_OK;
    }
    case OP_RANDOM_WALK: {
        u16 nl; const u8 *nm = rstr(r, &nl);
        u32 depth = r32(r); u8 dir = r8(r); u8 merw = r8(r); u64 seed = r64(r);
        if (r->err || depth > 256) { err_reply(w, "bad req"); return ST_ERR; }
        u32 eid = lookup_or0(k, nm, nl);
        if (!eid) { err_reply(w, "no such entity"); return ST_ERR; }
        u32 *path = (u32 *)malloc((size_t)(depth + 1) * 4);
        if (!path) { err_reply(w, "oom"); return ST_ERR; }
        u32 n = g4_random_walk(k->g, eid, depth, wire_dir(dir), merw, seed,
                               path, depth + 1);
        w32(w, n);
        for (u32 i = 0; i < n && i <= depth; i++) { put_name(k, w, path[i]); pw_add(k, path[i]); }
        free(path);
        return ST_OK;
    }
    default:
        err_reply(w, "unknown op");
        return ST_ERR;
    }
}

/* ---------- connections + event loop ---------- */

typedef struct {
    int fd, authed;
    u8 *in; u32 inlen, incap;
    u8 *out; u32 outlen, outoff, outcap;
} conn_t;

#define MAX_CONNS 64

static int conn_queue(conn_t *c, const u8 *data, u32 len) {
    if (c->outlen + len > c->outcap) {
        u32 nc = c->outcap ? c->outcap : 4096;
        while (nc < c->outlen + len) nc *= 2;
        u8 *nb = (u8 *)realloc(c->out, nc);
        if (!nb) return 0;
        c->out = nb; c->outcap = nc;
    }
    memcpy(c->out + c->outlen, data, len);
    c->outlen += len;
    return 1;
}

static int send_response(conn_t *c, u32 reqid, u8 status, const wr_t *w) {
    u8 hdr[9];
    u32 len = 5 + w->len;
    hdr[0]=(u8)len; hdr[1]=(u8)(len>>8); hdr[2]=(u8)(len>>16); hdr[3]=(u8)(len>>24);
    hdr[4]=(u8)reqid; hdr[5]=(u8)(reqid>>8); hdr[6]=(u8)(reqid>>16); hdr[7]=(u8)(reqid>>24);
    hdr[8]=status;
    return conn_queue(c, hdr, 9) && (w->len == 0 || conn_queue(c, w->buf, w->len));
}

/* process complete frames in c->in; returns 0 to close the connection */
static int conn_process(kbd_t *k, conn_t *c) {
    u32 off = 0;
    while (c->inlen - off >= 4) {
        const u8 *p = c->in + off;
        u32 flen = (u32)p[0]|((u32)p[1]<<8)|((u32)p[2]<<16)|((u32)p[3]<<24);
        if (flen < 5 || flen > KBD_MAX_FRAME) return 0;
        if (c->inlen - off < 4 + flen) break;
        u32 reqid = (u32)p[4]|((u32)p[5]<<8)|((u32)p[6]<<16)|((u32)p[7]<<24);
        u8 op = p[8];
        rd_t r = { p + 9, p + 4 + flen, 0 };
        wr_t w = { NULL, 0, 0, 0 };
        int ok = 1;
        if (!c->authed) {
            if (op == OP_AUTH) {
                u16 tl; const u8 *tok = rstr(&r, &tl);
                if (!r.err && tok && tl == k->token_len &&
                    memcmp(tok, k->token, tl) == 0) {
                    c->authed = 1;
                    ok = send_response(c, reqid, ST_OK, &w);
                } else ok = 0;                        /* bad token: hang up */
            } else ok = 0;                            /* op before auth */
        } else {
            int st = handle_op(k, op, &r, &w);
            if (w.err) { w.len = 0; st = ST_ERR; }
            ok = send_response(c, reqid, (u8)st, &w);
        }
        free(w.buf);
        if (!ok) return 0;
        off += 4 + flen;
    }
    if (off) {
        memmove(c->in, c->in + off, c->inlen - off);
        c->inlen -= off;
    }
    return 1;
}

static void conn_close(conn_t *c) {
    if (c->fd >= 0) close(c->fd);
    free(c->in); free(c->out);
    memset(c, 0, sizeof *c);
    c->fd = -1;
}

int kbd_serve(kbd_t *k, int lfd, volatile sig_atomic_t *stop) {
    conn_t conns[MAX_CONNS];
    for (int i = 0; i < MAX_CONNS; i++) { memset(&conns[i], 0, sizeof conns[i]); conns[i].fd = -1; }
    struct pollfd pfds[MAX_CONNS + 1];

    while (!*stop) {
        u32 np = 0;
        pfds[np].fd = lfd; pfds[np].events = POLLIN; np++;
        int idx_of[MAX_CONNS + 1];
        for (int i = 0; i < MAX_CONNS; i++) {
            if (conns[i].fd < 0) continue;
            pfds[np].fd = conns[i].fd;
            pfds[np].events = POLLIN | (conns[i].outlen > conns[i].outoff ? POLLOUT : 0);
            idx_of[np] = i;
            np++;
        }
        int rc = poll(pfds, np, 200);
        if (rc < 0) { if (errno == EINTR) continue; return 1; }
        if (pfds[0].revents & POLLIN) {
            int fd = accept(lfd, NULL, NULL);
            if (fd >= 0) {
                int one = 1;
                setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
                int placed = 0;
                for (int i = 0; i < MAX_CONNS; i++)
                    if (conns[i].fd < 0) { conns[i].fd = fd; placed = 1; break; }
                if (!placed) close(fd);
            }
        }
        for (u32 pi = 1; pi < np; pi++) {
            conn_t *c = &conns[idx_of[pi]];
            if (c->fd < 0) continue;
            if (pfds[pi].revents & (POLLERR | POLLHUP)) { conn_close(c); continue; }
            if (pfds[pi].revents & POLLIN) {
                if (c->incap - c->inlen < 4096) {
                    u32 nc = c->incap ? c->incap * 2 : 8192;
                    u8 *nb = (u8 *)realloc(c->in, nc);
                    if (!nb) { conn_close(c); continue; }
                    c->in = nb; c->incap = nc;
                }
                ssize_t got = read(c->fd, c->in + c->inlen, c->incap - c->inlen);
                if (got <= 0) { conn_close(c); continue; }
                c->inlen += (u32)got;
                if (c->inlen > KBD_MAX_FRAME + 16) { conn_close(c); continue; }
                if (!conn_process(k, c)) { conn_close(c); continue; }
            }
            if (c->fd >= 0 && c->outlen > c->outoff) {
                ssize_t sent = write(c->fd, c->out + c->outoff, c->outlen - c->outoff);
                if (sent < 0 && errno != EAGAIN && errno != EWOULDBLOCK) { conn_close(c); continue; }
                if (sent > 0) {
                    c->outoff += (u32)sent;
                    if (c->outoff == c->outlen) c->outoff = c->outlen = 0;
                }
            }
        }
    }
    for (int i = 0; i < MAX_CONNS; i++) if (conns[i].fd >= 0) conn_close(&conns[i]);
    /* final counter flush: piggyback on a dedicated txn */
    if (k->dirty_counters && mstore_txn_begin(k->ms)) {
        pw_flush_in_txn(k);
        mstore_txn_commit(k->ms);
    }
    return 0;
}

/* ---------- store open + main ---------- */

static volatile sig_atomic_t g_stop = 0;
static void on_term(int sig) { (void)sig; g_stop = 1; }

kbd_t *kbd_open(const char *dir) {
    char mp[512], gp[512], sp[512];
    snprintf(mp, sizeof mp, "%s/manifest.kb", dir);
    snprintf(gp, sizeof gp, "%s/graph.kb", dir);
    snprintf(sp, sizeof sp, "%s/strings.kb", dir);
    kbd_t *k = (kbd_t *)calloc(1, sizeof *k);
    if (!k) return NULL;
    int fresh = access(mp, F_OK) != 0;
    seg_io_t *mio = seg_io_posix_open(mp, 1);
    seg_io_t *sios[2] = { seg_io_posix_open(gp, 1), seg_io_posix_open(sp, 1) };
    if (!mio || !sios[0] || !sios[1]) { free(k); return NULL; }
    k->ms = fresh ? mstore_create(mio, sios, 2, 4)
                  : mstore_open(mio, sios, 2);
    if (!k->ms) { free(k); return NULL; }
    k->g = graph4_open(k->ms);
    if (!k->g) { mstore_close(k->ms); free(k); return NULL; }
    return k;
}

void kbd_close(kbd_t *k) {
    if (!k) return;
    graph4_close(k->g);
    mstore_close(k->ms);
    free(k->pw.eids); free(k->pw.walks);
    free(k);
}

#ifndef KBD_NO_MAIN
int main(int argc, char **argv) {
    if (argc != 4) {
        fprintf(stderr, "usage: kbd4 <store_dir> <port> <token_file>\n");
        return 2;
    }
    signal(SIGTERM, on_term);
    signal(SIGINT, on_term);
    signal(SIGPIPE, SIG_IGN);

    kbd_t *k = kbd_open(argv[1]);
    if (!k) { fprintf(stderr, "kbd4: store open failed\n"); return 1; }
    FILE *tf = fopen(argv[3], "rb");
    if (!tf) { fprintf(stderr, "kbd4: token file\n"); kbd_close(k); return 1; }
    size_t tl = fread(k->token, 1, sizeof k->token, tf);
    fclose(tf);
    while (tl && (k->token[tl - 1] == '\n' || k->token[tl - 1] == '\r')) tl--;
    if (!tl) { fprintf(stderr, "kbd4: empty token\n"); kbd_close(k); return 1; }
    k->token_len = (u16)tl;

    int lfd = socket(AF_INET, SOCK_STREAM, 0);
    int one = 1;
    setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof addr);
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons((unsigned short)atoi(argv[2]));
    if (bind(lfd, (struct sockaddr *)&addr, sizeof addr) != 0 || listen(lfd, 16) != 0) {
        fprintf(stderr, "kbd4: bind/listen: %s\n", strerror(errno));
        kbd_close(k); return 1;
    }
    socklen_t alen = sizeof addr;
    getsockname(lfd, (struct sockaddr *)&addr, &alen);
    printf("PORT %u\n", (unsigned)ntohs(addr.sin_port));
    fflush(stdout);

    int rc = kbd_serve(k, lfd, &g_stop);
    close(lfd);
    kbd_close(k);
    return rc;
}
#endif
