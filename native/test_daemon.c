/*
 * test_daemon.c — kbd4 end-to-end over real TCP: auth, full op battery,
 * the public-depth boundary, concurrent clients, restart persistence.
 *
 * The daemon runs in a forked child (kbd_open inside the child: no shared
 * mmaps/fds with the parent); the parent is the client.
 */
#define KBD_NO_MAIN
#include "seg_daemon.c"

#include <assert.h>
#include <sys/wait.h>

static int tests_run = 0;
#define TEST(name) do { printf("  %-52s", #name); tests_run++; } while (0)
#define PASS() printf("PASS\n")

/* ---- client side ---- */

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

static int cl_send(int fd, u32 reqid, u8 op, const wr_t *pl) {
    u8 hdr[9];
    u32 len = 5 + pl->len;
    hdr[0]=(u8)len; hdr[1]=(u8)(len>>8); hdr[2]=(u8)(len>>16); hdr[3]=(u8)(len>>24);
    hdr[4]=(u8)reqid; hdr[5]=(u8)(reqid>>8); hdr[6]=(u8)(reqid>>16); hdr[7]=(u8)(reqid>>24);
    hdr[8]=op;
    return wr_all(fd, hdr, 9) && (pl->len == 0 || wr_all(fd, pl->buf, pl->len));
}

/* receive one response; returns status or -1 on closed conn; fills body */
typedef struct { u8 *b; u32 n; } body_t;
static int cl_recv(int fd, u32 *reqid_out, body_t *body) {
    u8 hdr[9];
    if (!rd_all(fd, hdr, 9)) return -1;
    u32 len = (u32)hdr[0]|((u32)hdr[1]<<8)|((u32)hdr[2]<<16)|((u32)hdr[3]<<24);
    if (reqid_out)
        *reqid_out = (u32)hdr[4]|((u32)hdr[5]<<8)|((u32)hdr[6]<<16)|((u32)hdr[7]<<24);
    u8 status = hdr[8];
    u32 blen = len - 5;
    body->b = (u8 *)malloc(blen ? blen : 1);
    body->n = blen;
    if (blen && !rd_all(fd, body->b, blen)) { free(body->b); return -1; }
    return status;
}

/* one-shot: send + wait matching reqid */
static int cl_call(int fd, u32 reqid, u8 op, const wr_t *pl, body_t *body) {
    if (!cl_send(fd, reqid, op, pl)) return -1;
    u32 rid = 0;
    int st = cl_recv(fd, &rid, body);
    if (st >= 0) assert(rid == reqid);
    return st;
}

static void pl_reset(wr_t *w) { w->len = 0; w->err = 0; }
static void pl_str(wr_t *w, const char *s) { wstr(w, (const u8 *)s, (u16)strlen(s)); }

/* body reader mirroring rd_t */
static rd_t body_rd(const body_t *b) { rd_t r = { b->b, b->b + b->n, 0 }; return r; }
static void expect_name(rd_t *r, const char *want) {
    u16 l = 0; const u8 *s = rstr(r, &l);
    assert(s && l == strlen(want) && memcmp(s, want, l) == 0);
}

/* ---- daemon child ---- */

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
    if (pid == 0) {                      /* child: own store handle */
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

int main(void) {
    printf("test_daemon:\n");
    char dir[] = "/tmp/kbd4_test_XXXXXX";
    assert(mkdtemp(dir));
    const char *TOKEN = "sekrit-token-0451";
    unsigned short port = 0;
    pid_t pid = spawn_daemon(dir, TOKEN, &port);
    usleep(100000);

    wr_t pl = { NULL, 0, 0, 0 };
    body_t body;

    TEST(auth_gate);
    {
        /* wrong token: connection dropped */
        int fd = cl_connect(port);
        assert(fd >= 0);
        pl_reset(&pl); pl_str(&pl, "wrong");
        assert(cl_send(fd, 1, OP_AUTH, &pl));
        u32 rid; body_t b2;
        assert(cl_recv(fd, &rid, &b2) == -1);         /* hung up */
        close(fd);
        /* op before auth: dropped too */
        fd = cl_connect(port);
        pl_reset(&pl);
        assert(cl_send(fd, 1, OP_PING, &pl));
        assert(cl_recv(fd, &rid, &b2) == -1);
        close(fd);
    }
    PASS();

    int fd = cl_connect(port);
    assert(fd >= 0);
    pl_reset(&pl); pl_str(&pl, TOKEN);
    assert(cl_call(fd, 1, OP_AUTH, &pl, &body) == ST_OK);
    free(body.b);

    TEST(ping_and_stats_empty);
    {
        pl_reset(&pl);
        assert(cl_call(fd, 2, OP_PING, &pl, &body) == ST_OK);
        rd_t r = body_rd(&body);
        assert(r32(&r) == KBD_PROTO_VER);
        free(body.b);
        pl_reset(&pl);
        assert(cl_call(fd, 3, OP_STATS, &pl, &body) == ST_OK);
        r = body_rd(&body);
        assert(r32(&r) == 0 && r32(&r) == 0);
        free(body.b);
    }
    PASS();

    TEST(write_batches_and_open_nodes);
    {
        pl_reset(&pl);
        w32(&pl, 3);
        pl_str(&pl, "Self");    pl_str(&pl, "agent");     w64(&pl, 100);
        pl_str(&pl, "Melting"); pl_str(&pl, "process");   w64(&pl, 101);
        pl_str(&pl, "KB");      pl_str(&pl, "store");     w64(&pl, 102);
        assert(cl_call(fd, 10, OP_CREATE_ENTITIES, &pl, &body) == ST_OK);
        rd_t r = body_rd(&body);
        u32 e1 = r32(&r), e2 = r32(&r), e3 = r32(&r);
        assert(e1 && e2 && e3 && e1 != e2 && e2 != e3);
        free(body.b);

        pl_reset(&pl);
        w32(&pl, 2);
        pl_str(&pl, "Self"); pl_str(&pl, "Melting"); pl_str(&pl, "EXECUTES"); w64(&pl, 110);
        pl_str(&pl, "Melting"); pl_str(&pl, "KB"); pl_str(&pl, "REFINES"); w64(&pl, 111);
        assert(cl_call(fd, 11, OP_CREATE_RELATIONS, &pl, &body) == ST_OK);
        r = body_rd(&body);
        assert(r8(&r) == 1 && r8(&r) == 1);
        free(body.b);

        pl_reset(&pl);
        w32(&pl, 1);
        pl_str(&pl, "Self"); pl_str(&pl, "executable KB entity"); w64(&pl, 120);
        assert(cl_call(fd, 12, OP_ADD_OBS, &pl, &body) == ST_OK);
        r = body_rd(&body);
        assert(r8(&r) == 1);
        free(body.b);

        pl_reset(&pl);
        w32(&pl, 2);
        pl_str(&pl, "Self"); pl_str(&pl, "NoSuch");
        assert(cl_call(fd, 13, OP_OPEN_NODES, &pl, &body) == ST_OK);
        r = body_rd(&body);
        assert(r8(&r) == 1);                           /* Self found */
        expect_name(&r, "Self");
        expect_name(&r, "agent");
        assert(r64(&r) == 120);                        /* mtime updated by obs */
        assert(r64(&r) == 120);                        /* obs_mtime */
        assert(r8(&r) == 1);                           /* one obs */
        expect_name(&r, "executable KB entity");
        assert(r32(&r) == 1);                          /* one edge */
        expect_name(&r, "EXECUTES");
        assert(r8(&r) == 0);                           /* FORWARD */
        expect_name(&r, "Melting");
        assert(r64(&r) == 110);
        assert(r8(&r) == 0);                           /* NoSuch absent */
        free(body.b);
    }
    PASS();

    TEST(public_depth_boundary_and_traversal);
    {
        /* PUBLIC depth 0 = immediate only: Self -> {Melting} */
        pl_reset(&pl);
        pl_str(&pl, "Self"); w32(&pl, 0); w8(&pl, 2 /*ANY*/); w32(&pl, 16);
        assert(cl_call(fd, 20, OP_NEIGHBORS, &pl, &body) == ST_OK);
        rd_t r = body_rd(&body);
        assert(r32(&r) == 1);
        expect_name(&r, "Melting");
        free(body.b);
        /* PUBLIC depth 1 = two hops: {Melting, KB} */
        pl_reset(&pl);
        pl_str(&pl, "Self"); w32(&pl, 1); w8(&pl, 2); w32(&pl, 16);
        assert(cl_call(fd, 21, OP_NEIGHBORS, &pl, &body) == ST_OK);
        r = body_rd(&body);
        assert(r32(&r) == 2);
        free(body.b);
        /* find_path Self -> KB */
        pl_reset(&pl);
        pl_str(&pl, "Self"); pl_str(&pl, "KB"); w32(&pl, 8); w8(&pl, 0 /*FWD*/);
        assert(cl_call(fd, 22, OP_FIND_PATH, &pl, &body) == ST_OK);
        r = body_rd(&body);
        assert(r32(&r) == 3);
        expect_name(&r, "Self"); expect_name(&r, "Melting"); expect_name(&r, "KB");
        free(body.b);
        /* random walk from Self */
        pl_reset(&pl);
        pl_str(&pl, "Self"); w32(&pl, 5); w8(&pl, 0); w8(&pl, 0); w64(&pl, 4242);
        assert(cl_call(fd, 23, OP_RANDOM_WALK, &pl, &body) == ST_OK);
        r = body_rd(&body);
        u32 n = r32(&r);
        assert(n >= 1);
        expect_name(&r, "Self");
        free(body.b);
    }
    PASS();

    TEST(queries);
    {
        pl_reset(&pl);
        pl_str(&pl, "Melt|KB"); w32(&pl, 16);
        assert(cl_call(fd, 30, OP_SEARCH, &pl, &body) == ST_OK);
        rd_t r = body_rd(&body);
        assert(r32(&r) == 3);                          /* Melting, KB, Self(obs "KB") */
        free(body.b);
        pl_reset(&pl);
        pl_str(&pl, "process"); w32(&pl, 16);
        assert(cl_call(fd, 31, OP_BY_TYPE, &pl, &body) == ST_OK);
        r = body_rd(&body);
        assert(r32(&r) == 1);
        expect_name(&r, "Melting");
        free(body.b);
        pl_reset(&pl);
        w32(&pl, 16);
        assert(cl_call(fd, 32, OP_ENTITY_TYPES, &pl, &body) == ST_OK);
        r = body_rd(&body);
        assert(r32(&r) == 3);
        free(body.b);
        pl_reset(&pl);
        w32(&pl, 16);
        assert(cl_call(fd, 33, OP_RELATION_TYPES, &pl, &body) == ST_OK);
        r = body_rd(&body);
        assert(r32(&r) == 2);
        free(body.b);
        pl_reset(&pl);
        w32(&pl, 16);
        assert(cl_call(fd, 34, OP_ORPHANED, &pl, &body) == ST_OK);
        r = body_rd(&body);
        assert(r32(&r) == 0);
        free(body.b);
        /* invalid pattern is a clean error, not a hangup */
        pl_reset(&pl);
        pl_str(&pl, "([bad"); w32(&pl, 16);
        assert(cl_call(fd, 35, OP_SEARCH, &pl, &body) == ST_ERR);
        free(body.b);
        pl_reset(&pl);
        assert(cl_call(fd, 36, OP_PING, &pl, &body) == ST_OK);   /* conn alive */
        free(body.b);
    }
    PASS();

    TEST(second_client_concurrent);
    {
        int fd2 = cl_connect(port);
        assert(fd2 >= 0);
        pl_reset(&pl); pl_str(&pl, TOKEN);
        assert(cl_call(fd2, 1, OP_AUTH, &pl, &body) == ST_OK);
        free(body.b);
        /* interleave: send on both, then read both */
        pl_reset(&pl);
        assert(cl_send(fd, 40, OP_STATS, &pl));
        assert(cl_send(fd2, 41, OP_STATS, &pl));
        u32 rid;
        assert(cl_recv(fd, &rid, &body) == ST_OK && rid == 40);
        free(body.b);
        assert(cl_recv(fd2, &rid, &body) == ST_OK && rid == 41);
        rd_t r = body_rd(&body);
        assert(r32(&r) == 3 && r32(&r) == 2);
        free(body.b);
        /* a write from client 2 is visible to client 1 */
        pl_reset(&pl);
        w32(&pl, 1);
        pl_str(&pl, "FromC2"); pl_str(&pl, "t"); w64(&pl, 200);
        assert(cl_call(fd2, 42, OP_CREATE_ENTITIES, &pl, &body) == ST_OK);
        free(body.b);
        pl_reset(&pl);
        w32(&pl, 1); pl_str(&pl, "FromC2");
        assert(cl_call(fd, 43, OP_OPEN_NODES, &pl, &body) == ST_OK);
        r = body_rd(&body);
        assert(r8(&r) == 1);
        free(body.b);
        close(fd2);
    }
    PASS();

    TEST(restart_persistence);
    {
        close(fd);
        stop_daemon(pid);
        pid = spawn_daemon(dir, TOKEN, &port);
        usleep(100000);
        fd = cl_connect(port);
        assert(fd >= 0);
        pl_reset(&pl); pl_str(&pl, TOKEN);
        assert(cl_call(fd, 1, OP_AUTH, &pl, &body) == ST_OK);
        free(body.b);
        pl_reset(&pl);
        assert(cl_call(fd, 2, OP_STATS, &pl, &body) == ST_OK);
        rd_t r = body_rd(&body);
        assert(r32(&r) == 4 && r32(&r) == 2);          /* everything survived */
        free(body.b);
        pl_reset(&pl);
        w32(&pl, 1); pl_str(&pl, "Self");
        assert(cl_call(fd, 3, OP_OPEN_NODES, &pl, &body) == ST_OK);
        r = body_rd(&body);
        assert(r8(&r) == 1);
        expect_name(&r, "Self");
        expect_name(&r, "agent");
        free(body.b);
        /* delete works after restart (name index + strings rebuilt) */
        pl_reset(&pl);
        w32(&pl, 1); pl_str(&pl, "FromC2");
        assert(cl_call(fd, 4, OP_DELETE_ENTITIES, &pl, &body) == ST_OK);
        r = body_rd(&body);
        assert(r8(&r) == 1);
        free(body.b);
    }
    PASS();

    TEST(ranks_resample_validate_scan);
    {
        /* RESAMPLE: structural sample + MERW psi in one txn */
        pl_reset(&pl);
        assert(cl_call(fd, 50, OP_RESAMPLE, &pl, &body) == ST_OK);
        assert(body.n == 0);
        free(body.b);

        /* RANKS: totals + per-name values; unknown name -> zeros */
        pl_reset(&pl);
        w32(&pl, 2); pl_str(&pl, "Self"); pl_str(&pl, "NoSuchName");
        assert(cl_call(fd, 51, OP_RANKS, &pl, &body) == ST_OK);
        rd_t r = body_rd(&body);
        u64 wtot = r64(&r), stot = r64(&r);
        assert(stot > 0);                                  /* resample populated it */
        double self_srank;
        { u64 b2 = r64(&r); (void)b2; }                    /* walker rank */
        { u64 b2 = r64(&r); memcpy(&self_srank, &b2, 8); } /* structural rank */
        { u64 b2 = r64(&r); (void)b2; }                    /* psi */
        assert(self_srank >= 0.0);
        double missing_wr;
        { u64 b2 = r64(&r); memcpy(&missing_wr, &b2, 8); } /* NoSuchName walker rank */
        r64(&r); r64(&r);
        assert(missing_wr == 0.0);
        (void)wtot;
        free(body.b);

        /* VALIDATE: flag exactly one oversized observation */
        pl_reset(&pl);
        w32(&pl, 1); pl_str(&pl, "BadObs"); pl_str(&pl, "t"); w64(&pl, 300);
        assert(cl_call(fd, 52, OP_CREATE_ENTITIES, &pl, &body) == ST_OK);
        free(body.b);
        pl_reset(&pl);
        char longobs[160];
        memset(longobs, 'x', 155); longobs[155] = 0;
        w32(&pl, 1); pl_str(&pl, "BadObs"); pl_str(&pl, longobs); w64(&pl, 301);
        assert(cl_call(fd, 53, OP_ADD_OBS, &pl, &body) == ST_OK);
        free(body.b);
        pl_reset(&pl);
        assert(cl_call(fd, 54, OP_VALIDATE, &pl, &body) == ST_OK);
        r = body_rd(&body);
        assert(r32(&r) == 0);                              /* no missing entities */
        assert(r32(&r) == 1);                              /* one violation */
        expect_name(&r, "BadObs");
        assert(r8(&r) == 1 && r8(&r) == 1);                /* count 1, over_mask bit 0 */
        free(body.b);

        /* SCAN: drain the KB in chunks of 2; every entity exactly once */
        u32 seen = 0, cur = 0, guard = 0;
        do {
            pl_reset(&pl);
            w32(&pl, cur); w32(&pl, 2);
            assert(cl_call(fd, 60 + guard, OP_SCAN, &pl, &body) == ST_OK);
            r = body_rd(&body);
            cur = r32(&r);
            u32 cnt = r32(&r);
            for (u32 i = 0; i < cnt; i++) {
                r32(&r);                                   /* eid */
                u16 l; rstr(&r, &l);                       /* name */
                rstr(&r, &l);                              /* type */
                u8 oc = r8(&r);
                for (u8 o = 0; o < oc; o++) rstr(&r, &l);
                seen++;
            }
            free(body.b);
        } while (cur != 0 && ++guard < 200);
        assert(guard < 200);
        pl_reset(&pl);
        assert(cl_call(fd, 80, OP_STATS, &pl, &body) == ST_OK);
        r = body_rd(&body);
        u32 ents = r32(&r);
        free(body.b);
        assert(seen == ents);
    }
    PASS();

    TEST(random_walk_avoid_cycles);
    {
        pl_reset(&pl);
        w32(&pl, 2);
        pl_str(&pl, "LoopL"); pl_str(&pl, "t"); w64(&pl, 400);
        pl_str(&pl, "LoopR"); pl_str(&pl, "t"); w64(&pl, 401);
        assert(cl_call(fd, 90, OP_CREATE_ENTITIES, &pl, &body) == ST_OK);
        free(body.b);
        pl_reset(&pl);
        w32(&pl, 2);
        pl_str(&pl, "LoopL"); pl_str(&pl, "LoopR"); pl_str(&pl, "loops"); w64(&pl, 402);
        pl_str(&pl, "LoopR"); pl_str(&pl, "LoopL"); pl_str(&pl, "loops"); w64(&pl, 402);
        assert(cl_call(fd, 91, OP_CREATE_RELATIONS, &pl, &body) == ST_OK);
        free(body.b);
        for (int mode = 0; mode < 2; mode++) {
            pl_reset(&pl);
            pl_str(&pl, "LoopL"); w32(&pl, 5); w8(&pl, 0); w8(&pl, 1); w64(&pl, 7);
            w8(&pl, (u8)mode);                             /* avoid_cycles */
            assert(cl_call(fd, 92 + (u32)mode, OP_RANDOM_WALK, &pl, &body) == ST_OK);
            rd_t r = body_rd(&body);
            u32 n = r32(&r);
            for (u32 i = 0; i < n; i++) { u16 l; rstr(&r, &l); }
            u32 us = r32(&r);                              /* v1.1 trailer present */
            free(body.b);
            assert(us >= 1);                               /* psi 0 on fresh nodes -> fallback counted */
            if (mode == 0) assert(n == 6);                 /* cycles freely: L,R,L,R,L,R */
            else           assert(n == 2);                 /* self-avoiding: L,R then stop */
        }
    }
    PASS();

    TEST(search_skip_paging_and_regex_valid);
    {
        pl_reset(&pl);
        w32(&pl, 4);
        pl_str(&pl, "Skip1"); pl_str(&pl, "skipT"); w64(&pl, 500);
        pl_str(&pl, "Skip2"); pl_str(&pl, "skipT"); w64(&pl, 501);
        pl_str(&pl, "Skip3"); pl_str(&pl, "skipT"); w64(&pl, 502);
        pl_str(&pl, "Skip4"); pl_str(&pl, "skipT"); w64(&pl, 503);
        assert(cl_call(fd, 100, OP_CREATE_ENTITIES, &pl, &body) == ST_OK);
        free(body.b);

        /* page 1 (no skip trailer: old-style) + page 2 (skip=2): disjoint, together all 4 */
        char got[4][16]; u32 nseen = 0;
        for (int pg = 0; pg < 2; pg++) {
            pl_reset(&pl);
            pl_str(&pl, "skipT"); w32(&pl, 2);
            if (pg) w32(&pl, 2);                     /* v1.2 skip trailer */
            assert(cl_call(fd, 101 + (u32)pg, OP_BY_TYPE, &pl, &body) == ST_OK);
            rd_t r = body_rd(&body);
            assert(r32(&r) == 4);                    /* total across pages */
            u32 cnt = 0;
            while (r.p < r.end) {
                u16 l; const u8 *s = rstr(&r, &l);
                assert(s && l < 16);
                memcpy(got[nseen], s, l); got[nseen][l] = 0; nseen++;
                cnt++;
            }
            assert(cnt == 2);
            free(body.b);
        }
        assert(nseen == 4);
        for (u32 i = 0; i < 4; i++)
            for (u32 j = i + 1; j < 4; j++)
                assert(strcmp(got[i], got[j]) != 0);  /* no overlap across pages */

        /* REGEX_VALID: same ERE dialect as search */
        pl_reset(&pl); pl_str(&pl, "a+b");
        assert(cl_call(fd, 110, OP_REGEX_VALID, &pl, &body) == ST_OK);
        { rd_t r = body_rd(&body); assert(r8(&r) == 1); }
        free(body.b);
        pl_reset(&pl); pl_str(&pl, "([bad");
        assert(cl_call(fd, 111, OP_REGEX_VALID, &pl, &body) == ST_OK);
        { rd_t r = body_rd(&body); assert(r8(&r) == 0); }
        free(body.b);
    }
    PASS();

    TEST(create_reason_bytes);
    {
        /* v1.4: one reason byte per item after the eids (0=ok, 1=name over
         * record cap, 2=type over record cap, 3=other). Old readers ignore
         * the trailer. Kept at the END of the battery: it creates an entity
         * and must not perturb earlier counts (types/orphans). */
        static char big[9000];
        memset(big, 'n', sizeof big - 1);
        pl_reset(&pl);
        w32(&pl, 3);
        pl_str(&pl, "ReasonOk"); pl_str(&pl, "rt"); w64(&pl, 300);
        wstr(&pl, (const u8 *)big, (u16)8000); pl_str(&pl, "rt"); w64(&pl, 301);
        pl_str(&pl, "ReasonBadType"); wstr(&pl, (const u8 *)big, (u16)8000); w64(&pl, 302);
        assert(cl_call(fd, 14, OP_CREATE_ENTITIES, &pl, &body) == ST_OK);
        {
            rd_t r = body_rd(&body);
            u32 e1 = r32(&r), e2 = r32(&r), e3 = r32(&r);
            assert(e1 != 0 && e2 == 0 && e3 == 0);
            assert(r8(&r) == 0 && r8(&r) == 1 && r8(&r) == 2);
        }
        free(body.b);
    }
    PASS();

    TEST(find_path_beta_contract);
    {
        /* v1.3: the daemon FIND_PATH carries the v3 β-contract
         * (Decision_FindPathBetaContractInC): optional u64 budget trailer;
         * reply appends targetReached + budgetExhausted after the names;
         * best-effort path to the deepest discovered node when unreached. */
        pl_reset(&pl);
        w32(&pl, 6);
        for (int i = 0; i < 5; i++) {
            char nm[8]; snprintf(nm, sizeof nm, "FP%d", i);
            pl_str(&pl, nm); pl_str(&pl, "fpt"); w64(&pl, 900 + (u64)i);
        }
        pl_str(&pl, "FPIsland"); pl_str(&pl, "fpt"); w64(&pl, 950);
        assert(cl_call(fd, 200, OP_CREATE_ENTITIES, &pl, &body) == ST_OK);
        free(body.b);
        pl_reset(&pl);
        w32(&pl, 4);
        for (int i = 0; i < 4; i++) {
            char a[8], c[8];
            snprintf(a, sizeof a, "FP%d", i); snprintf(c, sizeof c, "FP%d", i + 1);
            pl_str(&pl, a); pl_str(&pl, c); pl_str(&pl, "next"); w64(&pl, 960 + (u64)i);
        }
        assert(cl_call(fd, 201, OP_CREATE_RELATIONS, &pl, &body) == ST_OK);
        free(body.b);

        /* (a) reachable + budget trailer: full path, flags (1,0) */
        pl_reset(&pl);
        pl_str(&pl, "FP0"); pl_str(&pl, "FP4"); w32(&pl, 4); w8(&pl, 0); w64(&pl, 1ull << 40);
        assert(cl_call(fd, 202, OP_FIND_PATH, &pl, &body) == ST_OK);
        {
            rd_t r = body_rd(&body);
            assert(r32(&r) == 5);
            for (int i = 0; i < 5; i++) { char nm[8]; snprintf(nm, sizeof nm, "FP%d", i); expect_name(&r, nm); }
            assert(r8(&r) == 1 && r8(&r) == 0);
        }
        free(body.b);

        /* (b) maxDepth rejection: best-effort partial to FP2, flags (0,0) */
        pl_reset(&pl);
        pl_str(&pl, "FP0"); pl_str(&pl, "FP4"); w32(&pl, 2); w8(&pl, 0); w64(&pl, 1ull << 40);
        assert(cl_call(fd, 203, OP_FIND_PATH, &pl, &body) == ST_OK);
        {
            rd_t r = body_rd(&body);
            assert(r32(&r) == 3);
            expect_name(&r, "FP0"); expect_name(&r, "FP1"); expect_name(&r, "FP2");
            assert(r8(&r) == 0 && r8(&r) == 0);
        }
        free(body.b);

        /* (c) budget 0: target-check-before-budget -> one discovery, flags (0,1) */
        pl_reset(&pl);
        pl_str(&pl, "FP0"); pl_str(&pl, "FP4"); w32(&pl, 10); w8(&pl, 0); w64(&pl, 0);
        assert(cl_call(fd, 204, OP_FIND_PATH, &pl, &body) == ST_OK);
        {
            rd_t r = body_rd(&body);
            assert(r32(&r) == 2);
            expect_name(&r, "FP0"); expect_name(&r, "FP1");
            assert(r8(&r) == 0 && r8(&r) == 1);
        }
        free(body.b);

        /* (d) legacy request without the trailer: unbounded, flags (1,0) */
        pl_reset(&pl);
        pl_str(&pl, "FP0"); pl_str(&pl, "FP4"); w32(&pl, 4); w8(&pl, 0);
        assert(cl_call(fd, 205, OP_FIND_PATH, &pl, &body) == ST_OK);
        {
            rd_t r = body_rd(&body);
            assert(r32(&r) == 5);
            for (int i = 0; i < 5; i++) { char nm[8]; snprintf(nm, sizeof nm, "FP%d", i); expect_name(&r, nm); }
            assert(r8(&r) == 1 && r8(&r) == 0);
        }
        free(body.b);

        /* (e) unreachable target: best-effort to the deepest discovered, flags (0,0) */
        pl_reset(&pl);
        pl_str(&pl, "FP0"); pl_str(&pl, "FPIsland"); w32(&pl, 8); w8(&pl, 0); w64(&pl, 1ull << 40);
        assert(cl_call(fd, 206, OP_FIND_PATH, &pl, &body) == ST_OK);
        {
            rd_t r = body_rd(&body);
            assert(r32(&r) == 5);                  /* FP0..FP4 = the whole reachable chain */
            expect_name(&r, "FP0"); expect_name(&r, "FP1"); expect_name(&r, "FP2");
            expect_name(&r, "FP3"); expect_name(&r, "FP4");
            assert(r8(&r) == 0 && r8(&r) == 0);
        }
        free(body.b);
    }
    PASS();

    TEST(background_rank_slice);
    {
        /* Spec §7 / Design_RankAmortized: rank is the owner's amortized
         * BACKGROUND job — this test never sends OP_RESAMPLE. cadence 0 makes
         * every dirty poll tick slice; a 4-iteration chunk forces the
         * warm-started ψ iteration to CONTINUE across ticks until the measured
         * |Δψ| < ε convergence (PR_PowerIteration), rather than stopping at
         * the chunk bound. */
        setenv("KBD_RANK_CADENCE_MS", "0", 1);
        setenv("KBD_RANK_SLICE_ITERS", "4", 1);
        char bdir[] = "/tmp/kbd4_test_XXXXXX";
        assert(mkdtemp(bdir));
        unsigned short bport = 0;
        pid_t bpid = spawn_daemon(bdir, TOKEN, &bport);
        unsetenv("KBD_RANK_CADENCE_MS");
        unsetenv("KBD_RANK_SLICE_ITERS");
        usleep(100000);

        int bfd = cl_connect(bport);
        assert(bfd >= 0);
        pl_reset(&pl); pl_str(&pl, TOKEN);
        assert(cl_call(bfd, 1, OP_AUTH, &pl, &body) == ST_OK);
        free(body.b);

        /* chain RB0→RB1→…→RB5: slow enough to need multiple slices at tol 1e-8 */
        pl_reset(&pl);
        w32(&pl, 6);
        for (int i = 0; i < 6; i++) {
            char nm[8]; snprintf(nm, sizeof nm, "RB%d", i);
            pl_str(&pl, nm); pl_str(&pl, "rankbg"); w64(&pl, 600 + (u64)i);
        }
        assert(cl_call(bfd, 2, OP_CREATE_ENTITIES, &pl, &body) == ST_OK);
        free(body.b);
        pl_reset(&pl);
        w32(&pl, 5);
        for (int i = 0; i < 5; i++) {
            char a[8], c[8];
            snprintf(a, sizeof a, "RB%d", i); snprintf(c, sizeof c, "RB%d", i + 1);
            pl_str(&pl, a); pl_str(&pl, c); pl_str(&pl, "NEXT"); w64(&pl, 700 + (u64)i);
        }
        assert(cl_call(bfd, 3, OP_CREATE_RELATIONS, &pl, &body) == ST_OK);
        free(body.b);

        /* Each RANKS round-trip is a poll tick; while dirty a slice runs before
         * the request is served. Track max |ψ change| between ticks. */
        double prev[6] = {0}, cur[6] = {0};
        int stable_at = -1;
        for (int tick = 0; tick < 400 && stable_at < 0; tick++) {
            pl_reset(&pl);
            w32(&pl, 6);
            for (int i = 0; i < 6; i++) { char nm[8]; snprintf(nm, sizeof nm, "RB%d", i); pl_str(&pl, nm); }
            assert(cl_call(bfd, 10 + (u32)tick, OP_RANKS, &pl, &body) == ST_OK);
            rd_t rr = body_rd(&body);
            (void)r64(&rr); (void)r64(&rr);            /* totals */
            double maxd = 0;
            for (int i = 0; i < 6; i++) {
                (void)r64(&rr); (void)r64(&rr);        /* walker, structural */
                u64 pb = r64(&rr);
                memcpy(&cur[i], &pb, 8);
                if (tick > 0) { double d = cur[i] - prev[i]; if (d < 0) d = -d; if (d > maxd) maxd = d; }
            }
            free(body.b);
            if (tick > 0 && maxd < 1e-9) stable_at = tick;
            memcpy(prev, cur, sizeof cur);
        }
        assert(stable_at >= 2);        /* converged over MULTIPLE slices, not one chunk */
        assert(cur[0] > 0.0 && cur[5] > 0.0);   /* ψ maintained with no op-path resample */

        close(bfd);
        stop_daemon(bpid);
        char bcmd[600];
        snprintf(bcmd, sizeof bcmd, "rm -rf %s", bdir);
        assert(system(bcmd) == 0);
    }
    PASS();

    TEST(find_path_continuation);
    {
        /* v1.5 leases, Phase A (spec §4 as r3.1): a budgeted find_path that
         * cuts mid-search issues a continuation token; RESUME continues via
         * exact replay + a fresh segment budget; failures are coded
         * (1=expired, 2=stale, 3=unknown); at cap (KBD_LEASE_MAX) no token is
         * issued. Chain LQ0→…→LQ8; per-discovery cost = 3+2+28 = 33B, root 31B. */
        setenv("KBD_LEASE_MAX", "2", 1);
        setenv("KBD_LEASE_TTL_MS", "60000", 1);
        char ldir[] = "/tmp/kbd4_test_XXXXXX";
        assert(mkdtemp(ldir));
        unsigned short lport = 0;
        pid_t lpid = spawn_daemon(ldir, TOKEN, &lport);
        unsetenv("KBD_LEASE_MAX");
        unsetenv("KBD_LEASE_TTL_MS");
        usleep(100000);
        int lfd = cl_connect(lport);
        assert(lfd >= 0);
        pl_reset(&pl); pl_str(&pl, TOKEN);
        assert(cl_call(lfd, 1, OP_AUTH, &pl, &body) == ST_OK);
        free(body.b);

        pl_reset(&pl);
        w32(&pl, 9);
        for (int i = 0; i < 9; i++) {
            char nm[8]; snprintf(nm, sizeof nm, "LQ%d", i);
            pl_str(&pl, nm); pl_str(&pl, "lbg"); w64(&pl, 1000 + (u64)i);
        }
        assert(cl_call(lfd, 2, OP_CREATE_ENTITIES, &pl, &body) == ST_OK);
        free(body.b);
        pl_reset(&pl);
        w32(&pl, 8);
        for (int i = 0; i < 8; i++) {
            char a[8], c[8];
            snprintf(a, sizeof a, "LQ%d", i); snprintf(c, sizeof c, "LQ%d", i + 1);
            pl_str(&pl, a); pl_str(&pl, c); pl_str(&pl, "NX"); w64(&pl, 1100 + (u64)i);
        }
        assert(cl_call(lfd, 3, OP_CREATE_RELATIONS, &pl, &body) == ST_OK);
        free(body.b);

        /* fresh run, budget 90: cum 31 → 64 (LQ1) → 97 ≥ 90 → trip at LQ2 */
        pl_reset(&pl);
        pl_str(&pl, "LQ0"); pl_str(&pl, "LQ8"); w32(&pl, 9); w8(&pl, 0); w64(&pl, 90);
        assert(cl_call(lfd, 4, OP_FIND_PATH, &pl, &body) == ST_OK);
        u64 t1 = 0;
        {
            rd_t r = body_rd(&body);
            u32 n = r32(&r);
            char last[8] = {0};
            for (u32 i = 0; i < n; i++) { u16 l; const u8 *s = rstr(&r, &l); if (l < 8) { memcpy(last, s, l); last[l] = 0; } }
            assert(n == 3 && strcmp(last, "LQ2") == 0);
            assert(r8(&r) == 0 && r8(&r) == 1);
            t1 = r64(&r);
            assert(t1 != 0);
        }
        free(body.b);

        /* RESUME(t1, 70): stop_at = 97+70 = 167 → LQ3 (130) LQ4 (163) push,
         * LQ5 (196) trips; path LQ0..LQ5; same lease id continues */
        pl_reset(&pl);
        w64(&pl, t1); w64(&pl, 70);
        assert(cl_call(lfd, 5, OP_RESUME, &pl, &body) == ST_OK);
        u64 t2 = 0;
        {
            rd_t r = body_rd(&body);
            u32 n = r32(&r);
            char last[8] = {0};
            for (u32 i = 0; i < n; i++) { u16 l; const u8 *s = rstr(&r, &l); if (l < 8) { memcpy(last, s, l); last[l] = 0; } }
            assert(n == 6 && strcmp(last, "LQ5") == 0);
            assert(r8(&r) == 0 && r8(&r) == 1);
            t2 = r64(&r);
            assert(t2 == t1);
        }
        free(body.b);

        /* RESUME(t2, big): replay to 196, then LQ6/LQ7 push; LQ8 discovery
         * hits the target check FIRST → reached; token retired */
        pl_reset(&pl);
        w64(&pl, t2); w64(&pl, 100000);
        assert(cl_call(lfd, 6, OP_RESUME, &pl, &body) == ST_OK);
        {
            rd_t r = body_rd(&body);
            assert(r32(&r) == 9);
            for (int i = 0; i < 9; i++) { char nm[8]; snprintf(nm, sizeof nm, "LQ%d", i); expect_name(&r, nm); }
            assert(r8(&r) == 1 && r8(&r) == 0);
            assert(r64(&r) == 0);
        }
        free(body.b);

        /* retired token: unknown */
        pl_reset(&pl);
        w64(&pl, t2); w64(&pl, 1000);
        assert(cl_call(lfd, 7, OP_RESUME, &pl, &body) == ST_ERR);
        { rd_t r = body_rd(&body); assert(r8(&r) == 3); }
        free(body.b);

        /* cap: two live tokens fill KBD_LEASE_MAX=2 → the third is refused (0) */
        u64 toks[3] = {0, 0, 0};
        for (int c = 0; c < 3; c++) {
            pl_reset(&pl);
            pl_str(&pl, "LQ0"); pl_str(&pl, "LQ8"); w32(&pl, 9); w8(&pl, 0); w64(&pl, 90);
            assert(cl_call(lfd, 20 + (u32)c, OP_FIND_PATH, &pl, &body) == ST_OK);
            rd_t r = body_rd(&body);
            u32 n = r32(&r);
            for (u32 i = 0; i < n; i++) { u16 l; (void)rstr(&r, &l); }
            assert(r8(&r) == 0 && r8(&r) == 1);
            toks[c] = r64(&r);
            free(body.b);
        }
        assert(toks[0] != 0 && toks[1] != 0 && toks[2] == 0);

        /* stale: a write advances the store txid; old tokens say so (code 2) */
        pl_reset(&pl);
        w32(&pl, 1);
        pl_str(&pl, "LQExtra"); pl_str(&pl, "lbg"); w64(&pl, 1300);
        assert(cl_call(lfd, 30, OP_CREATE_ENTITIES, &pl, &body) == ST_OK);
        free(body.b);
        pl_reset(&pl);
        w64(&pl, toks[0]); w64(&pl, 1000);
        assert(cl_call(lfd, 31, OP_RESUME, &pl, &body) == ST_ERR);
        { rd_t r = body_rd(&body); assert(r8(&r) == 2); }
        free(body.b);

        close(lfd);
        stop_daemon(lpid);
        char lcmd[600];
        snprintf(lcmd, sizeof lcmd, "rm -rf %s", ldir);
        assert(system(lcmd) == 0);
    }
    PASS();

    TEST(lease_expiry);
    {
        /* TTL=1ms daemon: a token older than the TTL replies code 1 */
        setenv("KBD_LEASE_TTL_MS", "1", 1);
        char edir[] = "/tmp/kbd4_test_XXXXXX";
        assert(mkdtemp(edir));
        unsigned short eport = 0;
        pid_t epid = spawn_daemon(edir, TOKEN, &eport);
        unsetenv("KBD_LEASE_TTL_MS");
        usleep(100000);
        int efd = cl_connect(eport);
        assert(efd >= 0);
        pl_reset(&pl); pl_str(&pl, TOKEN);
        assert(cl_call(efd, 1, OP_AUTH, &pl, &body) == ST_OK);
        free(body.b);

        pl_reset(&pl);
        w32(&pl, 4);
        for (int i = 0; i < 4; i++) {
            char nm[8]; snprintf(nm, sizeof nm, "EQ%d", i);
            pl_str(&pl, nm); pl_str(&pl, "lbg"); w64(&pl, 1400 + (u64)i);
        }
        assert(cl_call(efd, 2, OP_CREATE_ENTITIES, &pl, &body) == ST_OK);
        free(body.b);
        pl_reset(&pl);
        w32(&pl, 3);
        for (int i = 0; i < 3; i++) {
            char a[8], c[8];
            snprintf(a, sizeof a, "EQ%d", i); snprintf(c, sizeof c, "EQ%d", i + 1);
            pl_str(&pl, a); pl_str(&pl, c); pl_str(&pl, "NX"); w64(&pl, 1500 + (u64)i);
        }
        assert(cl_call(efd, 3, OP_CREATE_RELATIONS, &pl, &body) == ST_OK);
        free(body.b);

        /* budget 40: root 31, EQ1 at 64 ≥ 40 → trip, token issued */
        pl_reset(&pl);
        pl_str(&pl, "EQ0"); pl_str(&pl, "EQ3"); w32(&pl, 9); w8(&pl, 0); w64(&pl, 40);
        assert(cl_call(efd, 4, OP_FIND_PATH, &pl, &body) == ST_OK);
        u64 et = 0;
        {
            rd_t r = body_rd(&body);
            u32 n = r32(&r);
            for (u32 i = 0; i < n; i++) { u16 l; (void)rstr(&r, &l); }
            assert(r8(&r) == 0 && r8(&r) == 1);
            et = r64(&r);
            assert(et != 0);
        }
        free(body.b);

        usleep(5000);                      /* > 1ms TTL */
        pl_reset(&pl);
        w64(&pl, et); w64(&pl, 1000);
        assert(cl_call(efd, 5, OP_RESUME, &pl, &body) == ST_ERR);
        { rd_t r = body_rd(&body); assert(r8(&r) == 1); }
        free(body.b);

        close(efd);
        stop_daemon(epid);
        char ecmd[600];
        snprintf(ecmd, sizeof ecmd, "rm -rf %s", edir);
        assert(system(ecmd) == 0);
    }
    PASS();

    close(fd);
    stop_daemon(pid);
    free(pl.buf);

    char cmd[600];
    snprintf(cmd, sizeof cmd, "rm -rf %s", dir);
    assert(system(cmd) == 0);
    printf("test_daemon: %d tests passed\n", tests_run);
    return 0;
}
