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

    close(fd);
    stop_daemon(pid);
    free(pl.buf);

    char cmd[600];
    snprintf(cmd, sizeof cmd, "rm -rf %s", dir);
    assert(system(cmd) == 0);
    printf("test_daemon: %d tests passed\n", tests_run);
    return 0;
}
