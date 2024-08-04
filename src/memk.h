//
// Created by ozan on 03.08.2024.
//

#ifndef REDIS_MEMK_H
#define REDIS_MEMK_H

void memk_init(void);

void *memk_malloc(size_t size);
void *memk_malloc_usable(size_t size, size_t *usable);
void *memk_trymalloc(size_t size);
void *memk_trymalloc_usable(size_t size, size_t *usable);
void *memk_calloc(size_t size);
void *memk_realloc(void *ptr, size_t size);
void *memk_tryrealloc(void *ptr, size_t size);
void *memk_tryrealloc_usable(void *ptr, size_t size, size_t *usable);
void *memk_realloc_usable(void *ptr, size_t size, size_t *usable);
size_t memk_malloc_size(void *ptr);
void memk_free(void *ptr);

#endif //REDIS_MEMK_H
