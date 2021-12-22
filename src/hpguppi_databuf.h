/* hpguppi_databuf.h
 *
 * Defines shared mem structure for data passing.
 */
#ifndef _HPGUPPI_DATABUF_H
#define _HPGUPPI_DATABUF_H

#include <stdint.h>
#include "hashpipe_databuf.h"
#include "config.h"

// Technically we only need to align to 512 bytes,
// but this keeps things 4K (i.e. page) aligned.
#define ALIGNMENT_SIZE (4096)

#define N_INPUT_BLOCKS 24
#define BLOCK_HDR_SIZE  (5*80*512)      // in bytes, from guppi_daq_server
//#define BLOCK_DATA_SIZE (128*1024*1024) // in bytes, from guppi_daq_server (4k mode)
#define BLOCK_DATA_SIZE (128*1024*1024*8) // in bytes, from guppi_daq_server (32k mode)

typedef struct hpguppi_input_block {
  char hdr[BLOCK_HDR_SIZE];
  char data[BLOCK_DATA_SIZE];
} hpguppi_input_block_t;

// Used to pad after hashpipe_databuf_t to maintain data alignment
typedef uint8_t hashpipe_databuf_alignment[
  ALIGNMENT_SIZE - (sizeof(hashpipe_databuf_t)%ALIGNMENT_SIZE)
];

typedef struct hpguppi_input_databuf {
  hashpipe_databuf_t header;
  hashpipe_databuf_alignment padding; // Maintain data alignment
  hpguppi_input_block_t block[N_INPUT_BLOCKS];
} hpguppi_input_databuf_t;

/*
 * INPUT BUFFER FUNCTIONS
 */

hashpipe_databuf_t *hpguppi_input_databuf_create(int instance_id, int databuf_id);

static inline hpguppi_input_databuf_t *hpguppi_input_databuf_attach(int instance_id, int databuf_id)
{
    return (hpguppi_input_databuf_t *)hashpipe_databuf_attach(instance_id, databuf_id);
}

static inline int hpguppi_input_databuf_detach(hpguppi_input_databuf_t *d)
{
    return hashpipe_databuf_detach((hashpipe_databuf_t *)d);
}

static inline void hpguppi_input_databuf_clear(hpguppi_input_databuf_t *d)
{
    hashpipe_databuf_clear((hashpipe_databuf_t *)d);
}

static inline int hpguppi_input_databuf_block_status(hpguppi_input_databuf_t *d, int block_id)
{
    return hashpipe_databuf_block_status((hashpipe_databuf_t *)d, block_id);
}

static inline int hpguppi_input_databuf_total_status(hpguppi_input_databuf_t *d)
{
    return hashpipe_databuf_total_status((hashpipe_databuf_t *)d);
}

static inline int hpguppi_input_databuf_wait_free_timeout(
    hpguppi_input_databuf_t *d, int block_id, struct timespec *timeout)
{
    return hashpipe_databuf_wait_free_timeout((hashpipe_databuf_t *)d,
        block_id, timeout);
}

static inline int hpguppi_input_databuf_wait_free(hpguppi_input_databuf_t *d, int block_id)
{
    return hashpipe_databuf_wait_free((hashpipe_databuf_t *)d, block_id);
}

static inline int hpguppi_input_databuf_busywait_free(hpguppi_input_databuf_t *d, int block_id)
{
    return hashpipe_databuf_busywait_free((hashpipe_databuf_t *)d, block_id);
}

static inline int hpguppi_input_databuf_wait_filled_timeout(
    hpguppi_input_databuf_t *d, int block_id, struct timespec *timeout)
{
    return hashpipe_databuf_wait_filled_timeout((hashpipe_databuf_t *)d,
        block_id, timeout);
}

static inline int hpguppi_input_databuf_wait_filled(hpguppi_input_databuf_t *d, int block_id)
{
    return hashpipe_databuf_wait_filled((hashpipe_databuf_t *)d, block_id);
}

static inline int hpguppi_input_databuf_busywait_filled(hpguppi_input_databuf_t *d, int block_id)
{
    return hashpipe_databuf_busywait_filled((hashpipe_databuf_t *)d, block_id);
}

static inline int hpguppi_input_databuf_set_free(hpguppi_input_databuf_t *d, int block_id)
{
    return hashpipe_databuf_set_free((hashpipe_databuf_t *)d, block_id);
}

static inline int hpguppi_input_databuf_set_filled(hpguppi_input_databuf_t *d, int block_id)
{
    return hashpipe_databuf_set_filled((hashpipe_databuf_t *)d, block_id);
}

static inline char *hpguppi_databuf_header(struct hpguppi_input_databuf *d, int block_id) {
    if(block_id < 0 || d->header.n_block < block_id) {
        hashpipe_error(__FUNCTION__,
            "block_id %s out of range [0, %d)",
            block_id, d->header.n_block);
        return NULL;
    } else {
        return d->block[block_id].hdr;
    }
}

static inline char *hpguppi_databuf_data(struct hpguppi_input_databuf *d, int block_id) {
    if(block_id < 0 || d->header.n_block < block_id) {
        hashpipe_error(__FUNCTION__,
            "block_id %s out of range [0, %d)",
            block_id, d->header.n_block);
        return NULL;
    } else {
        return d->block[block_id].data;
    }
}

#if 0
/////////// OLD STUFF /////////////
/* Create a new shared mem area with given params.  Returns 
 * pointer to the new area on success, or NULL on error.  Returns
 * error if an existing shmem area exists with the given shmid (or
 * if other errors occured trying to allocate it).
 */
struct guppi_databuf *guppi_databuf_create(int n_block, size_t block_size,
        int databuf_id);

/* Return a pointer to a existing shmem segment with given id.
 * Returns error if segment does not exist 
 */
struct guppi_databuf *guppi_databuf_attach(int databuf_id);

/* Detach from shared mem segment */
int guppi_databuf_detach(struct guppi_databuf *d);

/* Clear out either the whole databuf (set all sems to 0, 
 * clear all header blocks) or a single FITS-style
 * header block.
 */
void guppi_databuf_clear(struct guppi_databuf *d);
void guppi_fitsbuf_clear(char *buf);

/* These return pointers to the header or data area for 
 * the given block_id.
 */
char *guppi_databuf_header(struct guppi_databuf *d, int block_id);
char *guppi_databuf_data(struct guppi_databuf *d, int block_id);

/* Returns lock status for given block_id, or total or bitmask or string for
 * whole array.
 */
int guppi_databuf_block_status(struct guppi_databuf *d, int block_id);
int guppi_databuf_total_status(struct guppi_databuf *d);
int guppi_databuf_bitmask_status(struct guppi_databuf *d);
void guppi_databuf_str_status(struct guppi_databuf *d, char * str, size_t len);

/* Databuf locking functions.  Each block in the buffer
 * can be marked as free or filled.  The "wait" functions
 * block until the specified state happens.  The "set" functions
 * put the buffer in the specified state, returning error if
 * it is already in that state.
 */
int guppi_databuf_wait_filled(struct guppi_databuf *d, int block_id);
int guppi_databuf_set_filled(struct guppi_databuf *d, int block_id);
int guppi_databuf_wait_free(struct guppi_databuf *d, int block_id);
int guppi_databuf_set_free(struct guppi_databuf *d, int block_id);
#endif // OLD STUFF


#endif // _HPGUPPI_DATABUF_H
