/*
 * N-API binding for memoryfile
 *
 * Exposes the C memoryfile allocator to Node.js.
 * Each MemoryFile handle is wrapped in a pointerless external.
 */

#define NAPI_VERSION 8
#include <node_api.h>
#include <stdlib.h>
#include <string.h>
#include "memoryfile.h"

/* =========================================================================
 * Helpers
 * ========================================================================= */

#define NAPI_CALL(call)                                                    \
    do {                                                                   \
        napi_status status = (call);                                       \
        if (status != napi_ok) {                                           \
            napi_throw_error(env, NULL, "N-API call failed: " #call);      \
            return NULL;                                                   \
        }                                                                  \
    } while (0)

static napi_value make_u64(napi_env env, u64 val) {
    /* Use BigInt for u64 to avoid precision loss */
    napi_value result;
    NAPI_CALL(napi_create_bigint_uint64(env, val, &result));
    return result;
}

static u64 get_u64(napi_env env, napi_value val) {
    bool lossless;
    uint64_t result;
    napi_get_value_bigint_uint64(env, val, &result, &lossless);
    return result;
}

static void mf_release(napi_env env, void *data, void *hint) {
    (void)env; (void)hint;
    memfile_t *mf = (memfile_t*)data;
    if (mf) {
        memfile_close(mf);  /* no-op if already closed */
        mf->mmap_base = NULL;
        mf->header = NULL;
        free(mf);
    }
}

static memfile_t *unwrap_mf(napi_env env, napi_value val) {
    memfile_t *mf;
    napi_get_value_external(env, val, (void**)&mf);
    return mf;
}

/* =========================================================================
 * memfile_open(path: string, initialSize: number) => external
 * ========================================================================= */

static napi_value n_memfile_open(napi_env env, napi_callback_info info) {
    size_t argc = 2;
    napi_value argv[2];
    NAPI_CALL(napi_get_cb_info(env, info, &argc, argv, NULL, NULL));

    /* Get path string */
    char path[4096];
    size_t path_len;
    NAPI_CALL(napi_get_value_string_utf8(env, argv[0], path, sizeof(path), &path_len));

    /* Get initial size */
    uint32_t initial_size;
    NAPI_CALL(napi_get_value_uint32(env, argv[1], &initial_size));

    memfile_t *mf = memfile_open(path, (size_t)initial_size);
    if (!mf) {
        napi_throw_error(env, NULL, "memfile_open failed");
        return NULL;
    }

    napi_value result;
    NAPI_CALL(napi_create_external(env, mf, mf_release, NULL, &result));
    return result;
}

/* =========================================================================
 * memfile_close(handle: external) => void
 * ========================================================================= */

static napi_value n_memfile_close(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value argv[1];
    NAPI_CALL(napi_get_cb_info(env, info, &argc, argv, NULL, NULL));

    memfile_t *mf = unwrap_mf(env, argv[0]);
    memfile_close(mf);

    return NULL;
}

/* =========================================================================
 * memfile_sync(handle: external) => void
 * ========================================================================= */

static napi_value n_memfile_sync(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value argv[1];
    NAPI_CALL(napi_get_cb_info(env, info, &argc, argv, NULL, NULL));

    memfile_t *mf = unwrap_mf(env, argv[0]);
    memfile_sync(mf);

    return NULL;
}

/* =========================================================================
 * memfile_alloc(handle: external, size: bigint) => bigint (offset)
 * ========================================================================= */

static napi_value n_memfile_alloc(napi_env env, napi_callback_info info) {
    size_t argc = 2;
    napi_value argv[2];
    NAPI_CALL(napi_get_cb_info(env, info, &argc, argv, NULL, NULL));

    memfile_t *mf = unwrap_mf(env, argv[0]);
    u64 size = get_u64(env, argv[1]);

    u64 offset = memfile_alloc(mf, size);
    return make_u64(env, offset);
}

/* =========================================================================
 * memfile_free(handle: external, offset: bigint) => void
 * ========================================================================= */

static napi_value n_memfile_free(napi_env env, napi_callback_info info) {
    size_t argc = 2;
    napi_value argv[2];
    NAPI_CALL(napi_get_cb_info(env, info, &argc, argv, NULL, NULL));

    memfile_t *mf = unwrap_mf(env, argv[0]);
    u64 offset = get_u64(env, argv[1]);

    memfile_free(mf, offset);

    return NULL;
}

/* =========================================================================
 * memfile_coalesce(handle: external) => void
 * ========================================================================= */

static napi_value n_memfile_coalesce(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value argv[1];
    NAPI_CALL(napi_get_cb_info(env, info, &argc, argv, NULL, NULL));

    memfile_t *mf = unwrap_mf(env, argv[0]);
    memfile_coalesce(mf);

    return NULL;
}

/* =========================================================================
 * memfile_read(handle: external, offset: bigint, length: bigint) => Buffer
 * ========================================================================= */

static napi_value n_memfile_read(napi_env env, napi_callback_info info) {
    size_t argc = 3;
    napi_value argv[3];
    NAPI_CALL(napi_get_cb_info(env, info, &argc, argv, NULL, NULL));

    memfile_t *mf = unwrap_mf(env, argv[0]);
    u64 offset = get_u64(env, argv[1]);
    u64 len = get_u64(env, argv[2]);

    /* Create a Node Buffer and copy data into it */
    void *buf_data;
    napi_value result;
    NAPI_CALL(napi_create_buffer(env, (size_t)len, &buf_data, &result));

    if (memfile_read(mf, offset, buf_data, len) < 0) {
        napi_throw_error(env, NULL, "memfile_read: offset/length out of bounds");
        return NULL;
    }

    return result;
}

/* =========================================================================
 * memfile_write(handle: external, offset: bigint, data: Buffer) => void
 * ========================================================================= */

static napi_value n_memfile_write(napi_env env, napi_callback_info info) {
    size_t argc = 3;
    napi_value argv[3];
    NAPI_CALL(napi_get_cb_info(env, info, &argc, argv, NULL, NULL));

    memfile_t *mf = unwrap_mf(env, argv[0]);
    u64 offset = get_u64(env, argv[1]);

    /* Get Buffer data */
    void *buf_data;
    size_t buf_len;
    NAPI_CALL(napi_get_buffer_info(env, argv[2], &buf_data, &buf_len));

    if (memfile_write(mf, offset, buf_data, buf_len) < 0) {
        napi_throw_error(env, NULL, "memfile_write: offset/length out of bounds");
        return NULL;
    }

    return NULL;
}

/* =========================================================================
 * memfile_lock_shared(handle: external) => void
 * ========================================================================= */

static napi_value n_memfile_lock_shared(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value argv[1];
    NAPI_CALL(napi_get_cb_info(env, info, &argc, argv, NULL, NULL));

    memfile_t *mf = unwrap_mf(env, argv[0]);
    if (memfile_lock_shared(mf) < 0) {
        napi_throw_error(env, NULL, "memfile_lock_shared failed");
        return NULL;
    }

    return NULL;
}

/* =========================================================================
 * memfile_lock_exclusive(handle: external) => void
 * ========================================================================= */

static napi_value n_memfile_lock_exclusive(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value argv[1];
    NAPI_CALL(napi_get_cb_info(env, info, &argc, argv, NULL, NULL));

    memfile_t *mf = unwrap_mf(env, argv[0]);
    if (memfile_lock_exclusive(mf) < 0) {
        napi_throw_error(env, NULL, "memfile_lock_exclusive failed");
        return NULL;
    }

    return NULL;
}

/* =========================================================================
 * memfile_unlock(handle: external) => void
 * ========================================================================= */

static napi_value n_memfile_unlock(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value argv[1];
    NAPI_CALL(napi_get_cb_info(env, info, &argc, argv, NULL, NULL));

    memfile_t *mf = unwrap_mf(env, argv[0]);
    if (memfile_unlock(mf) < 0) {
        napi_throw_error(env, NULL, "memfile_unlock failed");
        return NULL;
    }

    return NULL;
}

/* =========================================================================
 * memfile_stats(handle: external) => { fileSize, allocated, freeListHead }
 * ========================================================================= */

static napi_value n_memfile_stats(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value argv[1];
    NAPI_CALL(napi_get_cb_info(env, info, &argc, argv, NULL, NULL));

    memfile_t *mf = unwrap_mf(env, argv[0]);

    napi_value result;
    NAPI_CALL(napi_create_object(env, &result));

    napi_value v;
    NAPI_CALL(napi_create_bigint_uint64(env, mf->header->file_size, &v));
    NAPI_CALL(napi_set_named_property(env, result, "fileSize", v));

    NAPI_CALL(napi_create_bigint_uint64(env, mf->header->allocated, &v));
    NAPI_CALL(napi_set_named_property(env, result, "allocated", v));

    NAPI_CALL(napi_create_bigint_uint64(env, mf->header->free_list_head, &v));
    NAPI_CALL(napi_set_named_property(env, result, "freeListHead", v));

    return result;
}

/* =========================================================================
 * Module init
 * ========================================================================= */

#define EXPORT_FN(name, fn) do {                                               \
    napi_value _fn;                                                            \
    napi_create_function(env, name, NAPI_AUTO_LENGTH, fn, NULL, &_fn);         \
    napi_set_named_property(env, exports, name, _fn);                          \
} while(0)

NAPI_MODULE_INIT(/* napi_env env, napi_value exports */) {
    EXPORT_FN("open",          n_memfile_open);
    EXPORT_FN("close",         n_memfile_close);
    EXPORT_FN("sync",          n_memfile_sync);
    EXPORT_FN("alloc",         n_memfile_alloc);
    EXPORT_FN("free",          n_memfile_free);
    EXPORT_FN("coalesce",      n_memfile_coalesce);
    EXPORT_FN("read",          n_memfile_read);
    EXPORT_FN("write",         n_memfile_write);
    EXPORT_FN("lockShared",    n_memfile_lock_shared);
    EXPORT_FN("lockExclusive", n_memfile_lock_exclusive);
    EXPORT_FN("unlock",        n_memfile_unlock);
    EXPORT_FN("stats",         n_memfile_stats);
    return exports;
}
