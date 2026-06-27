#include "substrate.h"

#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>

static int mf_remap(mf_t *mf, size_t new_size) {
    if (ftruncate(mf->fd, new_size) < 0) return -1;
#ifdef __linux__
    void *nb = mremap(mf->base, mf->size, new_size, MREMAP_MAYMOVE);
    if (nb == MAP_FAILED) return -1;
#else
    munmap(mf->base, mf->size);
    void *nb = mmap(NULL, new_size, PROT_READ | PROT_WRITE, MAP_SHARED, mf->fd, 0);
    if (nb == MAP_FAILED) return -1;
#endif
    mf->base = nb;
    mf->size = new_size;
    mf->hdr = (mf_header_t *)nb;
    mf->hdr->file_size = new_size;
    return 0;
}

int mf_ensure(mf_t *mf, u64 need) {
    if (mf->hdr->allocated + need <= mf->hdr->file_size) return 0;
    size_t ns = mf->size * 2;
    if (ns < mf->hdr->allocated + need) ns = mf->hdr->allocated + need + 4096;
    return mf_remap(mf, ns);
}

mf_t *mf_open(const char *path, size_t initial_size) {
    mf_t *mf = calloc(1, sizeof(mf_t));
    if (!mf) return NULL;
    mf->fd = -1;
    mf->path = strdup(path);

    struct stat st;
    int exists = (stat(path, &st) == 0 && st.st_size > 0);

    if (exists) {
        mf->fd = open(path, O_RDWR);
        if (mf->fd < 0) goto fail;
        mf->size = (size_t)st.st_size;
        mf->base = mmap(NULL, mf->size, PROT_READ | PROT_WRITE, MAP_SHARED, mf->fd, 0);
        if (mf->base == MAP_FAILED) goto fail_fd;
        mf->hdr = (mf_header_t *)mf->base;
        if (mf->hdr->magic != MF_MAGIC) { munmap(mf->base, mf->size); goto fail_fd; }
    } else {
        mf->fd = open(path, O_RDWR | O_CREAT, 0644);
        if (mf->fd < 0) goto fail;
        if (initial_size < sizeof(mf_header_t) + 64) initial_size = 4096;
        if (ftruncate(mf->fd, initial_size) < 0) goto fail_fd;
        mf->size = initial_size;
        mf->base = mmap(NULL, mf->size, PROT_READ | PROT_WRITE, MAP_SHARED, mf->fd, 0);
        if (mf->base == MAP_FAILED) { unlink(path); goto fail_fd; }
        mf->hdr = (mf_header_t *)mf->base;
        mf->hdr->magic = MF_MAGIC;
        mf->hdr->version = MF_VERSION;
        mf->hdr->file_size = initial_size;
        mf->hdr->allocated = sizeof(mf_header_t);
        mf->hdr->free_root = 0;
        mf->hdr->free_bytes = 0;
        mf->hdr->free_count = 0;
    }
    return mf;

fail_fd:
    close(mf->fd);
fail:
    free(mf->path);
    free(mf);
    return NULL;
}

void mf_sync(mf_t *mf) {
    if (mf && !mf->closed && mf->base) msync(mf->base, mf->size, MS_SYNC);
}

void mf_close(mf_t *mf) {
    if (!mf || mf->closed) return;
    mf->closed = 1;
    if (mf->base) munmap(mf->base, mf->size);
    if (mf->fd >= 0) close(mf->fd);
    free(mf->path);
    free(mf);
}
