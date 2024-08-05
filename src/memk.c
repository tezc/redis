#include <assert.h>
#include <fcntl.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <stdio.h>
#include <sys/mman.h>
#include <string.h>

#include "memk.h"
#include "atomicvar.h"
#include "jemalloc/jemalloc.h"

extern redisAtomic size_t used_memory;

#define update_zmalloc_stat_alloc(__n) atomicIncr(used_memory,(__n))
#define update_zmalloc_stat_free(__n) atomicDecr(used_memory,(__n))

struct memk {
    unsigned char *addr;
    size_t size;
    size_t current;
    int arena;
    extent_hooks_t hooks;
} memk;

static void zmalloc_default_oom(size_t size) {
    fprintf(stderr, "zmalloc: Out of memory trying to allocate %zu bytes\n",
            size);
    fflush(stderr);
    abort();
}

static void (*zmalloc_oom_handler)(size_t) = zmalloc_default_oom;

pthread_mutex_t mtx = PTHREAD_MUTEX_INITIALIZER;

void *extent_alloc(extent_hooks_t *extent_hooks, void *new_addr, size_t size, size_t alignment, bool *zero, bool *commit, unsigned arena_ind) {
    void *addr = NULL;

    if (new_addr != NULL) {
        /* not supported */
        return NULL;
    }

    pthread_mutex_lock(&mtx);

    // calculate alignment offset
    size_t align_offset = 0u;
    size_t alignment_modulo = ((uintptr_t)memk.addr) % alignment;
    if (alignment_modulo != 0) {
        align_offset = alignment - alignment_modulo;
    }

    if (memk.current + size + align_offset > memk.size) {
        pthread_mutex_unlock(&mtx);
        return MAP_FAILED;
    }

    memk.addr += align_offset;
    addr = memk.addr;
    memk.addr += size;
    memk.current += size + align_offset;

    if (*zero)
        memset(memk.addr, 0, size);

    pthread_mutex_unlock(&mtx);

    *commit = true;
    return addr;
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

    unsigned int arena;
    size_t sz = sizeof(unsigned int);
    int err = je_mallctl("arenas.create", (void *)&arena, &sz, NULL, 0);
    if (err != 0) {
        abort();
    }

    char buf[512];
    snprintf(buf, sizeof(buf), "arena.%d.extent_hooks", arena);
    extent_hooks_t *hooks, *new;
    size_t len = sizeof(hooks);

    // Read the existing hooks
    err = je_mallctl(buf, &hooks, &len, NULL, 0);
    if (err) {
        abort();
    }

    //hooks->alloc = extent_alloc;

    memk.hooks = *hooks;
    memk.hooks.alloc = extent_alloc;

    new = &memk.hooks;

    err = je_mallctl(buf, NULL, NULL, &new, sizeof(new));
    if (err) {
        abort();
    }

    memk.arena = arena;
    memk.addr = addr;
    memk.size = FIXED_MAP_SIZE;
}

static inline void *memk_trymalloc_usable_internal(size_t size, size_t *usable) {
    /* Possible overflow, return NULL, so that the caller can panic or handle a failed allocation. */
    if (size >= SIZE_MAX/2) return NULL;
    void *ptr = je_mallocx(size, MALLOCX_ARENA(memk.arena) | MALLOCX_TCACHE_NONE);

    if (!ptr) return NULL;

    size = je_malloc_usable_size(ptr);
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
    void *ptr = je_mallocx(size, MALLOCX_ARENA(memk.arena) | MALLOCX_ZERO);
    if (ptr == NULL) return NULL;

    size = je_malloc_usable_size(ptr);
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

    oldsize = je_malloc_usable_size(ptr);
    newptr = je_rallocx(ptr,size, MALLOCX_ARENA(memk.arena));
    if (newptr == NULL) {
        if (usable) *usable = 0;
        return NULL;
    }

    update_zmalloc_stat_free(oldsize);
    size = je_malloc_usable_size(newptr);
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

    update_zmalloc_stat_free(je_malloc_usable_size(ptr));
    je_free(ptr);
}

size_t memk_malloc_size(void *ptr) {
    return je_malloc_usable_size(ptr);
}
