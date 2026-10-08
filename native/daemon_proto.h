/*
 * daemon_proto.h — kbd4 wire protocol (spec r3 §6.1, v1 preset verbs).
 *
 * Frame:    [u32 len][u32 reqid][u8 op][payload]      (len = 5 + payload)
 * Response: [u32 len][u32 reqid][u8 status][payload]
 * All integers little-endian. str = [u16 len][bytes]. Max frame 1 MiB.
 *
 * First frame on a connection MUST be OP_AUTH with the bearer token; any
 * other op before successful auth closes the connection.
 *
 * DEPTH ON THE WIRE IS PUBLIC 0-INDEXED (0 = immediate neighbors); the
 * daemon applies the single +1 translation into the C hop-count
 * (Fix_DepthDefault / spec §6.3). Directions: 0=FORWARD 1=BACKWARD 2=ANY.
 *
 * v1 ships the traversal PRESETS (NEIGHBORS/FIND_PATH/RANDOM_WALK/SEARCH…);
 * the generalized TRAVERSE/RESUME continuation verbs (§6.2) layer on the
 * same frames when multi-host sharding lands — presets are already defined
 * as TRAVERSE shorthands, so the op space stays compatible.
 *
 * v1.1 (same framing): adds OP_RANKS/OP_RESAMPLE/OP_VALIDATE/OP_SCAN (shim
 * parity: rank reads for sorting, one-shot rank refresh, validate_graph,
 * full-corpus iteration for kb_load IDF) and appends optional fields to
 * RANDOM_WALK (avoid_cycles in, uniform_steps out). Trailing-field evolution
 * is per spec r3 §6: the frame header is frozen; payloads may grow.
 */
#ifndef DAEMON_PROTO_H
#define DAEMON_PROTO_H

#define KBD_MAX_FRAME   (1u << 20)
#define KBD_PROTO_VER   1u

enum {
    OP_AUTH             = 0x01,   /* str token -> OK/ERR                     */
    OP_PING             = 0x02,   /* -> OK, payload = u32 proto_ver          */
    OP_STATS            = 0x03,   /* -> u32 entities, u32 relations, u64 txid*/

    OP_CREATE_ENTITIES  = 0x10,   /* u32 n x {str name, str type, u64 mtime} -> n x u32 eid   */
    OP_DELETE_ENTITIES  = 0x11,   /* u32 n x str name                        -> n x u8 ok     */
    OP_CREATE_RELATIONS = 0x12,   /* u32 n x {str f, str t, str rt, u64 mt}  -> n x u8 ok     */
    OP_DELETE_RELATIONS = 0x13,   /* u32 n x {str f, str t, str rt}          -> n x u8 ok     */
    OP_ADD_OBS          = 0x14,   /* u32 n x {str name, str obs, u64 mtime}  -> n x u8 ok     */
    OP_DEL_OBS          = 0x15,   /* u32 n x {str name, str obs, u64 mtime}  -> n x u8 ok     */

    OP_OPEN_NODES       = 0x20,   /* u32 n x str name -> n x entity blob (see below)          */
    OP_NEIGHBORS        = 0x21,   /* str name, u32 depth(PUBLIC), u8 dir, u32 max
                                     -> u32 total, k x str name                               */
    OP_FIND_PATH        = 0x22,   /* str from, str to, u32 maxdepth, u8 dir
                                     -> u32 n, n x str name (0 = no path)                     */
    OP_SEARCH           = 0x23,   /* str pattern, u32 max -> u32 total, k x str name          */
    OP_BY_TYPE          = 0x24,   /* str type, u32 max    -> u32 total, k x str name          */
    OP_ENTITY_TYPES     = 0x25,   /* u32 max -> u32 total, k x str                            */
    OP_RELATION_TYPES   = 0x26,   /* u32 max -> u32 total, k x str                            */
    OP_ORPHANED         = 0x27,   /* u32 max -> u32 total, k x str name                       */
    OP_RANDOM_WALK      = 0x28,   /* str start, u32 depth, u8 dir, u8 merw, u64 seed,
                                     [u8 avoid_cycles]  (v1.1 optional trailing field)
                                     -> u32 n, n x str name, [u32 uniform_steps] (v1.1 trailer) */
    OP_RANKS            = 0x29,   /* u32 n x str name
                                     -> u64 walker_total, u64 structural_total,
                                        n x {f64 walker_rank, f64 structural_rank, f64 psi} */
    OP_RESAMPLE         = 0x2a,   /* (empty) -> OK. Runs structural sample + MERW psi
                                     synchronously in one mstore txn (one-shot rank refresh). */
    OP_VALIDATE         = 0x2b,   /* (empty) -> u32 nmissing, n x str name,
                                        u32 nviol, n x {str name, u8 obs_count, u8 over_mask} */
    OP_SCAN             = 0x2c,   /* u32 after_eid (exclusive; 0 = from start), u32 max
                                     -> u32 next_eid (0 = drained), u32 n,
                                        n x {u32 eid, str name, str type, u8 obs_count,
                                             obs_count x str obs} */
};

/* OPEN_NODES entity blob (per requested name):
 *   u8 found; if found:
 *   str name, str type, u64 mtime, u64 obs_mtime,
 *   u8 obs_count, obs_count x str obs,
 *   u32 nedges, nedges x { str rel, u8 dir, str target_name, u64 mtime }   */

enum { ST_OK = 0, ST_ERR = 1 };   /* ERR payload = str message */

#endif /* DAEMON_PROTO_H */
