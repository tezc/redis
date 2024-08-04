#include <assert.h>
#include <fcntl.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <stdio.h>
#include <sys/mman.h>

#include "memk.h"
#include "memkind.h"
#include "atomicvar.h"

struct memkind *pmem_kind;
extern redisAtomic size_t used_memory;

#define update_zmalloc_stat_alloc(__n) atomicIncr(used_memory,(__n))
#define update_zmalloc_stat_free(__n) atomicDecr(used_memory,(__n))

static void zmalloc_default_oom(size_t size) {
    fprintf(stderr, "zmalloc: Out of memory trying to allocate %zu bytes\n",
            size);
    fflush(stderr);
    abort();
}

static void (*zmalloc_oom_handler)(size_t) = zmalloc_default_oom;

static void print_err_message(int err){
    char error_message[MEMKIND_ERROR_MESSAGE_SIZE];
    memkind_error_message(err, error_message, MEMKIND_ERROR_MESSAGE_SIZE);
    fprintf(stderr, "%s\n", error_message);
}


void memk_init(void) {
#define FIXED_MAP_SIZE (128ULL * 1024 * 1024 * 1024ULL)

    int fd = open("/home/ozan/Desktop/test.disk", O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    if (fd == -1) {
        abort();
    }

    posix_fallocate(fd, 0, FIXED_MAP_SIZE);

    void *addr = mmap(NULL, FIXED_MAP_SIZE, PROT_READ | PROT_WRITE,
                      MAP_SHARED, fd, 0);
    assert(addr != MAP_FAILED);

    int e = posix_madvise(addr, FIXED_MAP_SIZE, POSIX_MADV_RANDOM);
    if (e != 0) {
        abort();
    }

    int err = memkind_create_fixed(addr, FIXED_MAP_SIZE, &pmem_kind);
    if (err) {
        print_err_message(err);
        abort();
    }
}

static inline void *memk_trymalloc_usable_internal(size_t size, size_t *usable) {
    /* Possible overflow, return NULL, so that the caller can panic or handle a failed allocation. */
    if (size >= SIZE_MAX/2) return NULL;
    void *ptr = memkind_malloc(pmem_kind, size);

    if (!ptr) return NULL;

    size = memkind_malloc_usable_size(pmem_kind, ptr);
    update_zmalloc_stat_alloc(size);
    if (usable) *usable = size;
    return ptr;
}

void *memk_malloc_usable(size_t size, size_t *usable) {
    size_t usable_size = 0;
    void *ptr = memk_trymalloc_usable_internal(size, &usable_size);
    if (!ptr) zmalloc_oom_handler(size);
    if (usable) *usable = usable_size;
    return ptr;
}

void *memk_malloc(size_t size) {
    void *ptr = memk_trymalloc_usable_internal(size, NULL);
    if (!ptr) zmalloc_oom_handler(size);
    return ptr;
}

void *memk_trymalloc(size_t size) {
    void *ptr = memk_trymalloc_usable_internal(size, NULL);
    return ptr;
}

/* Try allocating memory and zero it, and return NULL if failed.
 * '*usable' is set to the usable size if non NULL. */
static inline void *memk_trycalloc_usable_internal(size_t size, size_t *usable) {
    /* Possible overflow, return NULL, so that the caller can panic or handle a failed allocation. */
    if (size >= SIZE_MAX/2) return NULL;
    void *ptr = memkind_calloc(pmem_kind, 1, size);
    if (ptr == NULL) return NULL;

    size = memkind_malloc_usable_size(pmem_kind, ptr);
    update_zmalloc_stat_alloc(size);
    if (usable) *usable = size;
    return ptr;
}

void *memk_calloc(size_t size) {
    void *ptr = memk_trycalloc_usable_internal(size, NULL);
    if (!ptr) zmalloc_oom_handler(size);
    return ptr;
}

void *memk_trymalloc_usable(size_t size, size_t *usable) {
    size_t usable_size = 0;
    void *ptr = memk_trymalloc_usable_internal(size, &usable_size);

    if (usable) *usable = usable_size;
    return ptr;
}

/* Try reallocating memory, and return NULL if failed.
 * '*usable' is set to the usable size if non NULL. */
static inline void *memk_tryrealloc_usable_internal(void *ptr, size_t size, size_t *usable) {
    size_t oldsize;
    void *newptr;

    /* not allocating anything, just redirect to free. */
    if (size == 0 && ptr != NULL) {
        memk_free(ptr);
        if (usable) *usable = 0;
        return NULL;
    }
    /* Not freeing anything, just redirect to malloc. */
    if (ptr == NULL)
        return memk_trymalloc_usable(size, usable);

    /* Possible overflow, return NULL, so that the caller can panic or handle a failed allocation. */
    if (size >= SIZE_MAX/2) {
        memk_free(ptr);
        if (usable) *usable = 0;
        return NULL;
    }

    oldsize = memkind_malloc_usable_size(pmem_kind, ptr);
    newptr = memkind_realloc(pmem_kind,ptr,size);
    if (newptr == NULL) {
        if (usable) *usable = 0;
        return NULL;
    }

    update_zmalloc_stat_free(oldsize);
    size = memkind_malloc_usable_size(pmem_kind, newptr);
    update_zmalloc_stat_alloc(size);
    if (usable) *usable = size;
    return newptr;
}

void *memk_tryrealloc_usable(void *ptr, size_t size, size_t *usable) {
    size_t usable_size = 0;
    ptr = memk_tryrealloc_usable_internal(ptr, size, &usable_size);
    if (usable) *usable = usable_size;
    return ptr;
}

void *memk_realloc_usable(void *ptr, size_t size, size_t *usable) {
    size_t usable_size = 0;
    ptr = memk_tryrealloc_usable(ptr, size, &usable_size);
    if (!ptr && size != 0) zmalloc_oom_handler(size);
    if (usable) *usable = usable_size;
    return ptr;
}

void *memk_realloc(void *ptr, size_t size) {
    ptr = memk_tryrealloc_usable_internal(ptr, size, NULL);
    if (!ptr && size != 0) zmalloc_oom_handler(size);
    return ptr;
}

/* Try Reallocating memory, and return NULL if failed. */
void *memk_tryrealloc(void *ptr, size_t size) {
    ptr = memk_tryrealloc_usable_internal(ptr, size, NULL);
    return ptr;
}

void memk_free(void *ptr) {
    if (ptr == NULL) return;

    update_zmalloc_stat_free(memkind_malloc_usable_size(pmem_kind, ptr));
    memkind_free(pmem_kind, ptr);
}

size_t memk_malloc_size(void *ptr) {
    return memkind_malloc_usable_size(pmem_kind, ptr);
}