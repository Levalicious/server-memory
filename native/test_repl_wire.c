/*
 * test_repl_wire.c — the anti-entropy round over the REAL daemon channel
 * (docs/shard-seam-design-note.md §8/§10, seam step 5b-i-β).
 *
 * The 5a-ii scenario, wire edition: the test owns store A (the round
 * driver's local side); kbd4 daemons hold store B (byte-clone + disconnected
 * edits). Proven here:
 *   1. auth_gate          — wrong token never authenticates.
 *   2. snapshot_replaced  — a second BEGIN replaces the peer's snapshot; an
 *                           older stream's CELLS see the gen mismatch and
 *                           fail the round cleanly (never decode garbage).
 *   3. wire_round_converges — g4_repl_round_wire(A, client→B-daemon): the
 *                           same convergence and the same counts as the
 *                           in-process round (test_repl), with the
 *                           watermark delete decided AT the daemon
 *                           (RE_EDGE_PULL "covered") and the guarded LWW
 *                           running at both ends.
 *   4. daemon_round_op    — two kbd4 daemons; OP_RE_ROUND makes daemon A
 *                           drive a fresh one-edge-per-side divergence to
 *                           convergence.
 * Ground truth after the rounds: stop the daemons, open both stores
 * in-process, extract + diff (adjacency diff == 0), then run one more
 * in-process round — transport equivalence — that must apply nothing.
 */
#define KBD_NO_MAIN
#include "seg_daemon.c"          /* kbd_open/kbd_serve for the daemon children */
#include "repl_client.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

static u32 tests_run = 0;
#define TEST(name) do { printf("  %-52s", #name); tests_run++; } while (0)
#define PASS() printf("PASS\n")

/* ---- symbol collectors + exact diff (test_repl pattern) ---- */

typedef struct { u8 *v; u32 n, cap; } symset_t;

static void collect(void *ctx, const u8 *s) {
    symset_t *x = (symset_t *)ctx;
    if (x->n == x->cap) {
        x->cap = x->cap ? x->cap * 2 : 1024;
        x->v = (u8 *)realloc(x->v, (size_t)x->cap * RIBLT_WIDTH);
        assert(x->v);
    }
    memcpy(x->v + (size_t)x->n * RIBLT_WIDTH, s, RIBLT_WIDTH);
    x->n++;
}
static int cmp_sym(const void *a, const void *b) { return memcmp(a, b, RIBLT_WIDTH); }
static void uniq_syms(symset_t *s) {
    u32 w = 0;
    for (u32 i = 0; i < s->n; i++) {
        if (w == 0 || memcmp(s->v + (size_t)(w - 1) * RIBLT_WIDTH,
                             s->v + (size_t)i * RIBLT_WIDTH, RIBLT_WIDTH) != 0)
            memcpy(s->v + (size_t)w++ * RIBLT_WIDTH, s->v + (size_t)i * RIBLT_WIDTH, RIBLT_WIDTH);
    }
    s->n = w;
}
static u32 set_diff(const u8 *A, u32 na, const u8 *B, u32 nb, u8 *out, u32 out_cap) {
    u32 i = 0, j = 0, w = 0;
    while (i < na || j < nb) {
        int c = (i < na && j < nb)
              ? memcmp(A + (size_t)i * RIBLT_WIDTH, B + (size_t)j * RIBLT_WIDTH, RIBLT_WIDTH)
              : (i < na ? -1 : 1);
        if (c < 0) { memcpy(out + (size_t)w++ * RIBLT_WIDTH, A + (size_t)i * RIBLT_WIDTH, RIBLT_WIDTH); i++; }
        else if (c > 0) { memcpy(out + (size_t)w++ * RIBLT_WIDTH, B + (size_t)j * RIBLT_WIDTH, RIBLT_WIDTH); j++; }
        else { i++; j++; }
        assert(w <= out_cap);
    }
    return w;
}

/* ---- store helpers (test_repl pattern) ---- */

static void copy_file(const char *src, const char *dst) {
    FILE *fi = fopen(src, "rb"); assert(fi);
    FILE *fo = fopen(dst, "wb"); assert(fo);
    char buf[65536]; size_t r;
    while ((r = fread(buf, 1, sizeof buf, fi)) > 0) assert(fwrite(buf, 1, r, fo) == r);
    fclose(fi); fclose(fo);
}

static mstore_t *open_store(const char *mp, const char *gp, const char *sp, int create) {
    seg_io_t *sios[2] = { seg_io_posix_open(gp, create), seg_io_posix_open(sp, create) };
    return create ? mstore_create(seg_io_posix_open(mp, 1), sios, 2, 2)
                  : mstore_open(seg_io_posix_open(mp, 0), sios, 2);
}

/* ---- daemon spawn (test_daemon pattern) ---- */

static pid_t spawn_daemon(const char *dir, const char *token, unsigned short *port_out) {
    int lfd = socket(AF_INET, SOCK_STREAM, 0);
    int one = 1;
    setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    struct sockaddr_in a;
    memset(&a, 0, sizeof a);
    a.sin_family = AF_INET;
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    a.sin_port = 0;
    assert(bind(lfd, (struct sockaddr *)&a, sizeof a) == 0);
    assert(listen(lfd, 8) == 0);
    socklen_t alen = sizeof a;
    getsockname(lfd, (struct sockaddr *)&a, &alen);
    *port_out = ntohs(a.sin_port);

    pid_t pid = fork();
    assert(pid >= 0);
    if (pid == 0) {
        signal(SIGTERM, on_term);
        kbd_t *k = kbd_open(dir);
        if (!k) _exit(1);
        strncpy(k->token, token, sizeof k->token - 1);
        k->token_len = (u16)strlen(token);
        kbd_serve(k, lfd, &g_stop);
        kbd_close(k);
        close(lfd);
        _exit(0);
    }
    close(lfd);
    return pid;
}

static void stop_daemon(pid_t pid) {
    kill(pid, SIGTERM);
    int st = 0;
    assert(waitpid(pid, &st, 0) == pid);
    assert(WIFEXITED(st) && WEXITSTATUS(st) == 0);
}

/* ---- raw client (for the daemon-side scenario + OP_RE_ROUND) ---- */

static void pl_reset(wr_t *w) { w->len = 0; w->err = 0; }
static void pl_str(wr_t *w, const char *s) { wstr(w, (const u8 *)s, (u16)strlen(s)); }

static int cl_connect(unsigned short port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in a;
    memset(&a, 0, sizeof a);
    a.sin_family = AF_INET;
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    a.sin_port = htons(port);
    if (connect(fd, (struct sockaddr *)&a, sizeof a) != 0) { close(fd); return -1; }
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
    return fd;
}
static int wr_all(int fd, const u8 *p, size_t n) {
    while (n) { ssize_t s = write(fd, p, n); if (s <= 0) return 0; p += s; n -= (size_t)s; }
    return 1;
}
static int rd_all(int fd, u8 *p, size_t n) {
    while (n) { ssize_t s = read(fd, p, n); if (s <= 0) return 0; p += s; n -= (size_t)s; }
    return 1;
}
static int cl_call(int fd, u32 reqid, u8 op, const wr_t *pl, u8 **body, u32 *blen) {
    u8 hdr[9];
    u32 len = 5 + pl->len;
    hdr[0]=(u8)len; hdr[1]=(u8)(len>>8); hdr[2]=(u8)(len>>16); hdr[3]=(u8)(len>>24);
    hdr[4]=(u8)reqid; hdr[5]=(u8)(reqid>>8); hdr[6]=(u8)(reqid>>16); hdr[7]=(u8)(reqid>>24);
    hdr[8]=op;
    if (!wr_all(fd, hdr, 9) || (pl->len && !wr_all(fd, pl->buf, pl->len))) return -1;
    if (!rd_all(fd, hdr, 9)) return -1;
    u32 rlen = (u32)hdr[0]|((u32)hdr[1]<<8)|((u32)hdr[2]<<16)|((u32)hdr[3]<<24);
    u8 status = hdr[8];
    if (rlen < 5) return -1;
    u32 rbl = rlen - 5;
    *body = NULL; *blen = 0;
    if (rbl) {
        *body = (u8 *)malloc(rbl);
        assert(*body);
        if (!rd_all(fd, *body, rbl)) { free(*body); *body = NULL; return -1; }
    }
    *blen = rbl;
    return status;
}
static int cl_auth(int fd, const char *token) {
    wr_t pl = { NULL, 0, 0, 0 };
    pl_str(&pl, token);
    u8 *body = NULL; u32 blen = 0;
    int st = cl_call(fd, 1, OP_AUTH, &pl, &body, &blen);
    free(pl.buf); free(body);
    return st == ST_OK;
}

int main(void) {
    printf("test_repl_wire:\n");
    assert(RIBLT_WIDTH == G4_SYM_LEN);
    const char *TOKEN = "repl-wire-token";

    char dirA[] = "/tmp/g4wire_A_XXXXXX", dirB[] = "/tmp/g4wire_B_XXXXXX";
    assert(mkdtemp(dirA) && mkdtemp(dirB));
    char mpa[256], gpa[256], spa[256], mpb[256], gpb[256], spb[256];
    snprintf(mpa, sizeof mpa, "%s/manifest.kb", dirA);
    snprintf(gpa, sizeof gpa, "%s/graph.kb", dirA);
    snprintf(spa, sizeof spa, "%s/strings.kb", dirA);
    snprintf(mpb, sizeof mpb, "%s/manifest.kb", dirB);
    snprintf(gpb, sizeof gpb, "%s/graph.kb", dirB);
    snprintf(spb, sizeof spb, "%s/strings.kb", dirB);

    /* ---- base store (400 entities, 300 relations, 60 obs) ---- */
    {
        mstore_t *ms = open_store(mpa, gpa, spa, 1);
        assert(ms);
        graph4_t *g = graph4_open(ms);
        assert(g);
        assert(mstore_txn_begin(ms));
        char nm[32], ty[16];
        u32 ids[400];
        for (u32 i = 0; i < 400; i++) {
            int n = snprintf(nm, sizeof nm, "RW_%04u", i);
            int t = snprintf(ty, sizeof ty, "T%u", i % 5u);
            ids[i] = g4_create_entity(g, (const u8 *)nm, (u16)n, (const u8 *)ty, (u16)t, 100 + i);
            assert(ids[i]);
            if (i < 60) assert(g4_add_observation(g, ids[i], (const u8 *)"base note", 9, 200 + i));
        }
        for (u32 i = 0; i < 300; i++) {
            char rt[16];
            int r = snprintf(rt, sizeof rt, "b%u", i % 3u);
            assert(g4_create_relation(g, ids[i], ids[(i * 7u + 3u) % 400u], (const u8 *)rt, (u16)r, 1000 + i));
        }
        assert(mstore_txn_commit(ms));
        graph4_close(g);
        mstore_close(ms);
    }
    copy_file(mpa, mpb); copy_file(gpa, gpb); copy_file(spa, spb);

    /* ---- A-side edits (mirror test_repl's round scenario) ---- */
    {
        mstore_t *ms = open_store(mpa, gpa, spa, 0);
        assert(ms);
        graph4_t *g = graph4_open(ms);
        assert(g);
        assert(mstore_txn_begin(ms));
        char nm[32];
        for (u32 i = 0; i < 3; i++) {
            int n = snprintf(nm, sizeof nm, "AX_%u", i);
            assert(g4_create_entity(g, (const u8 *)nm, (u16)n, (const u8 *)"T9", 2, 8000 + i));
        }
        u32 a1 = g4_lookup(g, (const u8 *)"RW_0001", 7);
        u32 a2 = g4_lookup(g, (const u8 *)"RW_0002", 7);
        u32 a3 = g4_lookup(g, (const u8 *)"RW_0003", 7);
        u32 a4 = g4_lookup(g, (const u8 *)"RW_0004", 7);
        assert(g4_create_relation(g, a1, a2, (const u8 *)"rA", 2, 9000));
        assert(g4_create_relation(g, a3, a4, (const u8 *)"rB", 2, 9001));
        u32 a10 = g4_lookup(g, (const u8 *)"RW_0010", 7);
        assert(g4_set_entity_fields(g, a10, 9000, 0, 0, 0, 0.0));
        u32 a20 = g4_lookup(g, (const u8 *)"RW_0020", 7);
        assert(g4_add_observation(g, a20, (const u8 *)"a-note", 6, 9000));
        assert(mstore_txn_commit(ms));
        graph4_close(g);
        mstore_close(ms);
    }
    /* ---- B-side edits (including the genuine edge delete) ---- */
    {
        mstore_t *ms = open_store(mpb, gpb, spb, 0);
        assert(ms);
        graph4_t *g = graph4_open(ms);
        assert(g);
        assert(mstore_txn_begin(ms));
        char nm[32];
        for (u32 i = 0; i < 2; i++) {
            int n = snprintf(nm, sizeof nm, "BX_%u", i);
            assert(g4_create_entity(g, (const u8 *)nm, (u16)n, (const u8 *)"T9", 2, 8100 + i));
        }
        u32 b10 = g4_lookup(g, (const u8 *)"RW_0010", 7);
        u32 b11 = g4_lookup(g, (const u8 *)"RW_0011", 7);
        assert(g4_create_relation(g, b10, b11, (const u8 *)"rC", 2, 9100));
        {   /* delete base relation 40: RW_0040 -> (40*7+3)%400 = 283, "b1" */
            u32 f = g4_lookup(g, (const u8 *)"RW_0040", 7);
            u32 t = g4_lookup(g, (const u8 *)"RW_0283", 7);
            assert(g4_delete_relation(g, f, t, (const u8 *)"b1", 2));
        }
        assert(g4_set_entity_fields(g, b10, 9200, 0, 0, 0, 0.0));   /* newer: LWW winner */
        u32 b25 = g4_lookup(g, (const u8 *)"RW_0025", 7);
        assert(g4_add_observation(g, b25, (const u8 *)"b-note", 6, 9100));
        assert(mstore_txn_commit(ms));
        graph4_close(g);
        mstore_close(ms);
    }

    unsigned short portB = 0;
    pid_t pidB = spawn_daemon(dirB, TOKEN, &portB);
    usleep(100000);

    TEST(auth_gate_wrong_token);
    {
        g4_wire_peer_t *bad = g4_wire_peer_open("127.0.0.1", portB, (const u8 *)"wrong", 5);
        assert(bad == NULL);
        g4_wire_peer_t *ok = g4_wire_peer_open("127.0.0.1", portB, (const u8 *)TOKEN, (u16)strlen(TOKEN));
        assert(ok);
        g4_wire_peer_close(ok);
    }
    PASS();

    TEST(snapshot_replaced_aborts_round);
    {
        g4_wire_peer_t *c1 = g4_wire_peer_open("127.0.0.1", portB, (const u8 *)TOKEN, (u16)strlen(TOKEN));
        g4_wire_peer_t *c2 = g4_wire_peer_open("127.0.0.1", portB, (const u8 *)TOKEN, (u16)strlen(TOKEN));
        assert(c1 && c2);
        repl_peer_t *i1 = g4_wire_peer_iface(c1);
        repl_peer_t *i2 = g4_wire_peer_iface(c2);
        u32 n1 = 0, n2 = 0;
        assert(i1->begin(i1->ctx, 0, &n1));
        assert(n1 > 0);
        assert(i2->begin(i2->ctx, 0, &n2));         /* replaces the snapshot  */
        assert(n2 == n1);                           /* same store state       */
        u8 buf[4 * 44];
        u32 n = 0;
        assert(!i1->cells(i1->ctx, 0, 0, 4, buf, &n));   /* stale gen: refused */
        assert(i1->begin(i1->ctx, 0, &n1));              /* re-anchor          */
        assert(i1->cells(i1->ctx, 0, 0, 4, buf, &n));
        assert(n == 4);
        i1->end(i1->ctx, 0);
        i2->end(i2->ctx, 0);
        g4_wire_peer_close(c1);
        g4_wire_peer_close(c2);
    }
    PASS();

    TEST(wire_round_converges);
    {
        mstore_t *ma = open_store(mpa, gpa, spa, 0);
        assert(ma);
        graph4_t *ga = graph4_open(ma);
        assert(ga);
        assert(mstore_txn_begin(ma));
        g4_wire_peer_t *peer = g4_wire_peer_open("127.0.0.1", portB, (const u8 *)TOKEN, (u16)strlen(TOKEN));
        assert(peer);
        g4_repl_stats_t st;
        assert(g4_repl_round_wire(ga, g4_wire_peer_iface(peer), &st));
        printf("(pulled_a=%u pulled_b=%u del_a=%u del_b=%u dup=%u skip=%u vs_a=%u vs_b=%u) ",
               st.edges_pulled_a, st.edges_pulled_b, st.edges_deleted_from_a,
               st.edges_deleted_from_b, st.edges_dup, st.edge_skipped,
               st.vstate_applied_a, st.vstate_applied_b);
        assert(st.edges_pulled_b == 2);            /* A's rA, rB into B       */
        assert(st.edges_pulled_a == 1);            /* B's rC into A           */
        assert(st.edges_deleted_from_a == 1);      /* B's delete, decided at B */
        assert(st.edges_deleted_from_b == 0);
        assert(st.edges_dup == 0 && st.edge_skipped == 0);
        assert(st.vstate_applied_a >= 2 && st.vstate_applied_b >= 1);
        assert(g4_chain_wm(ga, g4_lookup(ga, (const u8 *)"RW_0040", 7)) >= 1040);
        assert(mstore_txn_commit(ma));
        g4_wire_peer_close(peer);

        /* ground truth: stop the daemon, open B, diff both families */
        stop_daemon(pidB);
        mstore_t *mb = open_store(mpb, gpb, spb, 0);
        assert(mb);
        graph4_t *gb = graph4_open(mb);
        assert(gb);
        assert(mstore_txn_begin(mb));
        symset_t cA = {0}, cB = {0};
        assert(mstore_txn_begin(ma));
        g4_adj_symbols(ga, collect, &cA);
        g4_adj_symbols(gb, collect, &cB);
        assert(mstore_txn_commit(ma));
        qsort(cA.v, cA.n, RIBLT_WIDTH, cmp_sym); uniq_syms(&cA);
        qsort(cB.v, cB.n, RIBLT_WIDTH, cmp_sym); uniq_syms(&cB);
        u8 *t1 = (u8 *)malloc((size_t)(cA.n + cB.n) * RIBLT_WIDTH);
        u32 d1 = set_diff(cA.v, cA.n, cB.v, cB.n, t1, cA.n + cB.n);
        assert(d1 == 0);
        free(t1);
        free(cA.v); free(cB.v);

        /* transport equivalence: one in-process round applies nothing */
        assert(mstore_txn_begin(ma));
        g4_repl_stats_t st2;
        assert(g4_repl_round(ga, gb, &st2));
        assert(st2.edges_pulled_a == 0 && st2.edges_pulled_b == 0);
        assert(st2.edges_deleted_from_a == 0 && st2.edges_deleted_from_b == 0);
        assert(mstore_txn_commit(ma));
        assert(mstore_txn_commit(mb));
        graph4_close(ga); graph4_close(gb);
        mstore_close(ma); mstore_close(mb);
    }
    PASS();

    TEST(daemon_round_op);   /* two daemons; A drives via OP_RE_ROUND */
    {
        unsigned short portA2 = 0, portB2 = 0;
        pid_t pidA2 = spawn_daemon(dirA, TOKEN, &portA2);
        pid_t pidB2 = spawn_daemon(dirB, TOKEN, &portB2);
        usleep(100000);

        int fa = cl_connect(portA2), fb = cl_connect(portB2);
        assert(fa >= 0 && fb >= 0);
        assert(cl_auth(fa, TOKEN) && cl_auth(fb, TOKEN));

        /* fresh divergence: one edge per side, between base entities (the
         * entities themselves exist on both sides; edges are the fact) */
        {
            wr_t pl = { NULL, 0, 0, 0 };
            u8 *body = NULL; u32 blen = 0;
            w32(&pl, 1);
            pl_str(&pl, "RW_0100"); pl_str(&pl, "RW_0101"); pl_str(&pl, "wA"); w64(&pl, 12000);
            assert(cl_call(fa, 10, OP_CREATE_RELATIONS, &pl, &body, &blen) == ST_OK);
            free(body); body = NULL;
            pl_reset(&pl);
            w32(&pl, 1);
            pl_str(&pl, "RW_0200"); pl_str(&pl, "RW_0201"); pl_str(&pl, "wB"); w64(&pl, 12001);
            assert(cl_call(fb, 11, OP_CREATE_RELATIONS, &pl, &body, &blen) == ST_OK);
            free(body); body = NULL;
            free(pl.buf);
        }

        /* daemon A drives the round against daemon B */
        {
            wr_t pl = { NULL, 0, 0, 0 };
            pl_str(&pl, "127.0.0.1");
            w32(&pl, portB2);
            pl_str(&pl, TOKEN);
            u8 *body = NULL; u32 blen = 0;
            int st = cl_call(fa, 13, OP_RE_ROUND, &pl, &body, &blen);
            assert(st == ST_OK);
            assert(blen == 36);
            rd_t rr = { body, body + blen, 0 };
            u32 pulled_a = r32(&rr), pulled_b = r32(&rr);
            u32 del_a = r32(&rr), del_b = r32(&rr);
            u32 dup = r32(&rr), skip = r32(&rr);
            u32 vs_a = r32(&rr), vs_b = r32(&rr);
            (void)r32(&rr);                     /* vs_skip */
            printf("(pulled_a=%u pulled_b=%u del_a=%u del_b=%u dup=%u skip=%u vs_a=%u vs_b=%u) ",
                   pulled_a, pulled_b, del_a, del_b, dup, skip, vs_a, vs_b);
            assert(pulled_a == 1 && pulled_b == 1);   /* wB into A, wA into B */
            assert(del_a == 0 && del_b == 0 && dup == 0 && skip == 0);
            assert(vs_a >= 1 && vs_b >= 1);           /* the mtime bumps      */
            free(body);
            free(pl.buf);
        }
        close(fa);
        close(fb);
        stop_daemon(pidA2);
        stop_daemon(pidB2);

        /* ground truth + transport equivalence */
        mstore_t *ma = open_store(mpa, gpa, spa, 0);
        mstore_t *mb = open_store(mpb, gpb, spb, 0);
        assert(ma && mb);
        graph4_t *ga = graph4_open(ma);
        graph4_t *gb = graph4_open(mb);
        assert(ga && gb);
        assert(mstore_txn_begin(ma));
        assert(mstore_txn_begin(mb));
        symset_t cA = {0}, cB = {0};
        g4_adj_symbols(ga, collect, &cA);
        g4_adj_symbols(gb, collect, &cB);
        qsort(cA.v, cA.n, RIBLT_WIDTH, cmp_sym); uniq_syms(&cA);
        qsort(cB.v, cB.n, RIBLT_WIDTH, cmp_sym); uniq_syms(&cB);
        u8 *t1 = (u8 *)malloc((size_t)(cA.n + cB.n) * RIBLT_WIDTH);
        u32 d1 = set_diff(cA.v, cA.n, cB.v, cB.n, t1, cA.n + cB.n);
        assert(d1 == 0);
        free(t1); free(cA.v); free(cB.v);
        g4_repl_stats_t st2;
        assert(g4_repl_round(ga, gb, &st2));
        assert(st2.edges_pulled_a == 0 && st2.edges_pulled_b == 0);
        assert(mstore_txn_commit(ma));
        assert(mstore_txn_commit(mb));
        graph4_close(ga); graph4_close(gb);
        mstore_close(ma); mstore_close(mb);
    }
    PASS();

    {
        char cmd[600];
        snprintf(cmd, sizeof cmd, "rm -rf %s %s", dirA, dirB);
        assert(system(cmd) == 0);
    }
    printf("test_repl_wire: %u tests passed\n", tests_run);
    return 0;
}
