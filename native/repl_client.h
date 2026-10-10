/*
 * repl_client.h — the anti-entropy wire client (seam 5b-i-β): a repl_peer_t
 * (segstore.h) whose peer is a kbd4 daemon reached over TCP with the
 * daemon_proto.h framing. One connection = one round driver's view of the
 * peer; begin/cells/end track the peer's snapshot generation and refuse a
 * replaced stream (cells under a stale gen fail the round cleanly).
 *
 * Sockets carry SO_RCVTIMEO/SO_SNDTIMEO (G4_REPL_IO_TIMEOUT_MS, env
 * KBD_REPL_TIMEOUT_MS) so a hung peer fails the round instead of wedging
 * the caller — the driver may be a daemon's own event loop.
 */
#ifndef REPL_CLIENT_H
#define REPL_CLIENT_H

#include "segstore.h"

#define G4_REPL_IO_TIMEOUT_MS 30000   /* per-syscall socket timeout (ms) */

typedef struct g4_wire_peer g4_wire_peer_t;

/* connect + auth; NULL on connect failure or bad token (peer hangs up). */
g4_wire_peer_t *g4_wire_peer_open(const char *host, unsigned short port,
                                  const u8 *token, u16 tlen);
void g4_wire_peer_close(g4_wire_peer_t *p);
/* the repl_peer_t vtable (valid until close) */
repl_peer_t *g4_wire_peer_iface(g4_wire_peer_t *p);

#endif /* REPL_CLIENT_H */
