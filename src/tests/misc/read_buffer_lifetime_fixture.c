/* Verify single-read response storage is released after its bulk transfer. */
#include <dlfcn.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include "mercury_bulk.h"

static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
static void *          live[1024];
static unsigned        observed, outstanding;
typedef hg_return_t (*bulk_transfer_fn)(hg_context_t *, hg_cb_t, void *, hg_bulk_op_t, hg_addr_t, hg_bulk_t,
                                        hg_size_t, hg_bulk_t, hg_size_t, hg_size_t, hg_op_id_t *);

static pthread_once_t   symbols_once = PTHREAD_ONCE_INIT;
static bulk_transfer_fn original_transfer;
static hg_cb_t          single_read_callback;
static void *(*original_free)(void *);

static void
resolve_symbols(void)
{
    original_transfer    = (bulk_transfer_fn)dlsym(RTLD_NEXT, "HG_Bulk_transfer");
    single_read_callback = (hg_cb_t)dlsym(RTLD_NEXT, "transfer_request_bulk_transfer_read_cb");
    original_free        = (void *(*)(void *))dlsym(RTLD_NEXT, "PDC_free");
    if (original_transfer == NULL || single_read_callback == NULL || original_free == NULL)
        abort();
}

static void
record_buffer(void *buffer)
{
    pthread_mutex_lock(&mutex);
    for (unsigned i = 0; i < 1024; i++) {
        if (live[i] == buffer) {
            pthread_mutex_unlock(&mutex);
            return; /* A completed response buffer can be reused before shutdown. */
        }
    }
    for (unsigned i = 0; i < 1024; i++) {
        if (live[i] == NULL) {
            live[i] = buffer;
            observed++;
            outstanding++;
            pthread_mutex_unlock(&mutex);
            return;
        }
    }
    abort();
}

hg_return_t
HG_Bulk_transfer(hg_context_t *context, hg_cb_t callback, void *arg, hg_bulk_op_t op, hg_addr_t origin,
                 hg_bulk_t origin_handle, hg_size_t origin_offset, hg_bulk_t local_handle,
                 hg_size_t local_offset, hg_size_t size, hg_op_id_t *op_id)
{
    pthread_once(&symbols_once, resolve_symbols);
    if (op == HG_BULK_PUSH && callback == single_read_callback) {
        void *      buffer;
        hg_size_t   bytes;
        hg_uint32_t count;
        if (HG_Bulk_access(local_handle, local_offset, size, HG_BULK_READ_ONLY, 1, &buffer, &bytes, &count) !=
                HG_SUCCESS ||
            count != 1 || bytes != size)
            abort();
        record_buffer(buffer);
    }
    return original_transfer(context, callback, arg, op, origin, origin_handle, origin_offset, local_handle,
                             local_offset, size, op_id);
}

void *
PDC_free(void *pointer)
{
    pthread_once(&symbols_once, resolve_symbols);
    pthread_mutex_lock(&mutex);
    for (unsigned i = 0; i < 1024; i++) {
        if (pointer != NULL && live[i] == pointer) {
            live[i] = NULL;
            outstanding--;
            break;
        }
    }
    pthread_mutex_unlock(&mutex);
    return original_free(pointer);
}

__attribute__((destructor)) static void
verify_lifetime(void)
{
    fprintf(stderr, "PDC read-buffer lifetime: buffers_observed=%u outstanding=%u\n", observed, outstanding);
    if (observed == 0 || outstanding != 0) {
        fprintf(stderr, "[ERROR] Server read-buffer ownership was not satisfied\n");
        _exit(EXIT_FAILURE);
    }
}
