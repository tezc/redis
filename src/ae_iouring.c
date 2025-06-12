/* Linux epoll(2) based ae.c module
 *
 * Copyright (c) 2009-2012, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */


#include <sys/epoll.h> //used for define EPOLLIN and EPOLLOUT
#include <sys/resource.h>
#include "liburing.h"

#define MAX_ENTRIES 8192 /* entries should be configured by users */
#define ENABLE_SQPOLL 1

static int register_files = 1;

typedef struct uring_event {
    int fd;
    int type;
} uring_event;

typedef struct aeApiState {
    int urfd;
    struct io_uring *ring;
    struct uring_event *events;
} aeApiState;

static int aeApiCreate(aeEventLoop *eventLoop) {
    aeApiState *state = zmalloc(sizeof(aeApiState));
    if (!state) return -1;
    
    state->events = zmalloc(sizeof(struct epoll_event)*eventLoop->setsize);
    if (!state->events) {
        zfree(state);
        return -1;
    }
    
    state->ring = zmalloc(sizeof(struct io_uring));
    if (!state->ring) {
        zfree(state->events);
        zfree(state);
        return -1;
    }
    
    struct io_uring_params params;
    memset(&params, 0, sizeof(params));

    params.flags |= IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_COOP_TASKRUN | IORING_SETUP_DEFER_TASKRUN;

    if (eventLoop->extflags & ENABLE_SQPOLL)
        params.flags = IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_SQPOLL;

    state->urfd = io_uring_queue_init_params(MAX_ENTRIES, state->ring, &params);
    if (state->urfd != 0) {
        printf("error awas %d \n", state->urfd);
        zfree(state->ring);
        zfree(state->events);
        zfree(state);
        return -1;
    }
    if (!(params.features & IORING_FEAT_FAST_POLL)) {
        io_uring_queue_exit(state->ring);
        zfree(state->ring);
        zfree(state->events);
        zfree(state);
        return -1;
    }

    eventLoop->apidata = state;

    if (register_files) {
        int *files = malloc(MAX_ENTRIES * sizeof(int));
        if (!files) {
            register_files = 0;
            return 0;
        }
        for (int i = 0; i < MAX_ENTRIES; i++)
            files[i] = -1;
        int ret = io_uring_register_files(state->ring, files, MAX_ENTRIES);
        if (ret < 0) {
            fprintf(stderr, "error register_files %d\n", ret);
            register_files = 0;
        }
    }

    unsigned int rr[2] = {1, eventLoop->num_threads};

    int ret = io_uring_register(state->ring->ring_fd, IORING_REGISTER_IOWQ_MAX_WORKERS, rr, 2);
    if (ret != 0) {
        printf("Faield %d \n", ret);
    }

    return 0;
}

static int aeApiResize(aeEventLoop *eventLoop, int setsize) {
    (void)eventLoop;
    if (setsize >= FD_SETSIZE) return -1;
    return 0;
}

static void aeApiFree(aeEventLoop *eventLoop) {
    aeApiState *state = eventLoop->apidata;

    close(state->urfd);
    zfree(state->events);
    zfree(state->ring);
    zfree(state);
}

static int aeApiAddEvent(aeEventLoop *eventLoop, int fd, int mask,
                         struct iovec *iovecs) {
    aeApiState *state = eventLoop->apidata;

    struct io_uring_sqe *sqe = io_uring_get_sqe(state->ring);
    if (!sqe) return -1;

    /* NULL iovec means doing poll_add behavior */
    if (!iovecs) {
        unsigned int poll_mask = 0;
        if (mask == AE_READABLE) poll_mask |= EPOLLIN;
        if (mask == AE_WRITABLE) poll_mask |= EPOLLOUT;
        io_uring_prep_poll_add(sqe, fd, poll_mask);
    } else {
        if (mask & AE_READABLE) {
            //io_uring_prep_readv(sqe, fd, iovecs, 1, 0);
            io_uring_prep_read(sqe, fd, iovecs->iov_base, iovecs->iov_len, 0);
        }
        if (mask & AE_WRITABLE) {
            //io_uring_prep_writev(sqe, fd, iovecs, 1, 0);
            io_uring_prep_write(sqe, fd, iovecs->iov_base, iovecs->iov_len, 0);
        }

        io_uring_sqe_set_flags(sqe, IOSQE_ASYNC);
    }
    
    uring_event *ev = &state->events[fd];
    ev->fd = fd;
    ev->type = mask;
    if (!iovecs)
        ev->type |= AE_POLLABLE;
    if (register_files)
        sqe->flags |= IOSQE_FIXED_FILE;
    io_uring_sqe_set_data(sqe, (void *)ev);
    
    return 0;
}

static void aeApiDelEvent(aeEventLoop *eventLoop, int fd, int delmask) {
    (void)delmask;
    aeApiState *state = eventLoop->apidata;

    struct io_uring_sqe *sqe = io_uring_get_sqe(state->ring);
    if (!sqe) exit(1);
    
    uring_event *ev = &state->events[fd];
    io_uring_prep_poll_remove(sqe, (uintptr_t) ev);
}

static int aeApiPoll(aeEventLoop *eventLoop, struct timeval *tvp) {
    aeApiState *state = eventLoop->apidata;
    int retval, numevents = 0;
    struct io_uring_cqe *cqe;

    struct __kernel_timespec ts = {
            .tv_nsec = tvp->tv_usec * 1000,
            .tv_sec = tvp->tv_sec
    };

    retval = io_uring_submit_and_wait_timeout(state->ring, &cqe, 1, &ts, NULL);
    if (retval == -ETIME)
        return 0;
    else if (retval < 0) {
        fprintf(stderr, "submit_and_wait: %d\n", retval);
        return 0;
    }

    unsigned int num_completed = 0;
    unsigned int head;

    io_uring_for_each_cqe(state->ring, head, cqe) {
        int mask = 0;

        ++num_completed;
        uring_event *ev = io_uring_cqe_get_data(cqe);
        if (!ev) {
            abort();
        }

        if (ev->type & AE_POLLABLE) {
            if (cqe->res < 0) {
                //io_uring_cqe_seen(state->ring, cqe);
                continue;
            }

            if (cqe->res & EPOLLIN) mask |= AE_READABLE | AE_POLLABLE;
            if (cqe->res & EPOLLOUT) mask |= AE_WRITABLE | AE_POLLABLE;
            if (cqe->res & EPOLLERR) mask |= AE_WRITABLE | AE_READABLE | AE_POLLABLE;
            if (cqe->res & EPOLLHUP) mask |= AE_WRITABLE | AE_READABLE | AE_POLLABLE;
        } else {
            mask = ev->type;
            eventLoop->fired[numevents].res = cqe->res;
        }

        eventLoop->fired[numevents].fd = ev->fd;
        eventLoop->fired[numevents].mask = mask;
        numevents++;
    }

    if (num_completed)
        io_uring_cq_advance(state->ring, num_completed);

    return numevents;
}

void aeApiRegisterFile(aeEventLoop *eventLoop, int fd){
    aeApiState *state = eventLoop->apidata;
    if (register_files) {
        int ret = io_uring_register_files_update(state->ring, fd, &fd, 1);
        if (ret < 0) {
               fprintf(stderr, "lege io_uring_register_files_update failed: %d %d\n", fd, ret);
               register_files = 0;
        }
    }
}

static char *aeApiName(void) {
    return "io_uring";
}
