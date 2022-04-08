/* hpguppi_rawdisk_only_thread.c
 *
 * Write databuf blocks out to disk.
 */

#define _GNU_SOURCE 1
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>

#include "ioprio.h"

#include "hashpipe.h"

#include "hpguppi_databuf.h"
#include "hpguppi_params.h"
//#include "hpguppi_pksuwl.h"
#include "hpguppi_util.h"

// 80 character string for the BACKEND header record.
static const char BACKEND_RECORD[] =
// 0000000000111111111122222222223333333333
// 0123456789012345678901234567890123456789
  "BACKEND = 'GUPPI   '                    " \
  "                                        ";

#ifndef DEBUG_RAWSPEC_CALLBACKS
#define DEBUG_RAWSPEC_CALLBACKS (0)
#endif

static ssize_t write_all(int fd, const void *buf, size_t bytes_to_write)
{
  size_t bytes_remaining = bytes_to_write;
  ssize_t bytes_written = 0;
  while(bytes_remaining != 0) {
    bytes_written = write(fd, buf, bytes_remaining);
    if(bytes_written == -1) {
      // Error!
      return -1;
    }
    bytes_remaining -= bytes_written;
    buf += bytes_written;
  }
  // All done!
  return bytes_to_write;
}

static int safe_close(int *pfd) {
    if (pfd==NULL) return 0;
    fsync(*pfd);
    return close(*pfd);
}

#if 0
static int init(hashpipe_thread_args_t * args)
{
    int i;
    uint32_t Nc = 0;
    uint32_t Nbps = 8;

    hpguppi_input_databuf_t *db = (hpguppi_input_databuf_t *)args->ibuf;
    hashpipe_status_t *st = &args->st;

    hashpipe_status_lock_safe(st);
    // Get Nc from OBSNCHAN
    hgetu4(st->buf, "OBSNCHAN", &Nc);
    // Get Nbps from NBITS
    hgetu4(st->buf, "NBITS", &Nbps);
    hashpipe_status_unlock_safe(st);

    if(Nc == 0) {
      hashpipe_error("hpguppi_rawdisk_thread",
	  "OBSNCHAN not found in status buffer");
      return HASHPIPE_ERR_PARAM;
    }

    return HASHPIPE_OK;
}
#endif // 0

static void *run(hashpipe_thread_args_t * args)
{
    // Local aliases to shorten access to args fields
    // Our output buffer happens to be a hpguppi_input_databuf
    hpguppi_input_databuf_t *db = (hpguppi_input_databuf_t *)args->ibuf;
    hashpipe_status_t *st = &args->st;
    const char * thread_name = args->thread_desc->name;
    const char * status_key = args->thread_desc->skey;

    /* Read in general parameters */
    struct hpguppi_params gp;
    struct psrfits pf;
    pf.sub.dat_freqs = NULL;
    pf.sub.dat_weights = NULL;
    pf.sub.dat_offsets = NULL;
    pf.sub.dat_scales = NULL;
    pthread_cleanup_push((void *)hpguppi_free_psrfits, &pf);

    /* Init output file descriptor (-1 means no file open) */
    static int fdraw = -1;
    pthread_cleanup_push((void *)safe_close, &fdraw);

    /* Set I/O priority class for this thread to "real time" */
    if(ioprio_set(IOPRIO_WHO_PROCESS, 0, IOPRIO_PRIO_VALUE(IOPRIO_CLASS_RT, 7))) {
      hashpipe_error(thread_name, "ioprio_set IOPRIO_CLASS_RT");
    }

    /* Loop */
    int64_t pktidx=0, pktstart=0, pktstop=0;
    int blocksize=0, len=0;
    int curblock=0;
    int block_count=0, blocks_per_file=128, filenum=0;
    int got_packet_0=0, first=1;
    char *ptr, *hend;
    int open_flags = 0;
    int directio = 0;
    int rv = 0;

    while (run_threads()) {

        /* Note waiting status */
        hashpipe_status_lock_safe(st);
        hputs(st->buf, status_key, "waiting");
        hashpipe_status_unlock_safe(st);

        /* Wait for buf to have data */
        rv = hpguppi_input_databuf_wait_filled(db, curblock);
        if (rv!=0) continue;

        /* Read param struct for this block */
        ptr = hpguppi_databuf_header(db, curblock);
        if (first) {
            hpguppi_read_obs_params(ptr, &gp, &pf);
            first = 0;
        } else {
            hpguppi_read_subint_params(ptr, &gp, &pf);
        }

        /* Read pktidx, pktstart, pktstop from header */
        hgeti8(ptr, "PKTIDX", &pktidx);
        hgeti8(ptr, "PKTSTART", &pktstart);
        hgeti8(ptr, "PKTSTOP", &pktstop);

	// If packet idx is NOT within start/stop range
	if(pktidx < pktstart || pktstop <= pktidx) {
	    // If file open, close it
	    if(fdraw != -1) {
		// Close file
		close(fdraw);
		// Reset fdraw, got_packet_0, filenum, block_count
		fdraw = -1;
		got_packet_0 = 0;
		filenum = 0;
		block_count=0;

		// Print end of recording conditions
		hashpipe_info(thread_name, "recording stopped: "
		    "pktstart %lu pktstop %lu pktidx %lu",
		    pktstart, pktstop, pktidx);
	    }
	    /* Mark as free */
	    hpguppi_input_databuf_set_free(db, curblock);

	    /* Go to next block */
	    curblock = (curblock + 1) % db->header.n_block;

	    continue;
	}

        /* Set up data ptr for quant routines */
        pf.sub.data = (unsigned char *)hpguppi_databuf_data(db, curblock);

        // Wait for packet 0 before starting write
	// "packet 0" is the first packet/block of the new recording,
	// it is not necessarily pktidx == 0.
        if (got_packet_0==0 && gp.stt_valid==1) {
            got_packet_0 = 1;
            hpguppi_read_obs_params(ptr, &gp, &pf);
            directio = hpguppi_read_directio_mode(ptr);
            char fname[256];
            sprintf(fname, "%s.%04d.raw", pf.basefilename, filenum);
            fprintf(stderr, "Opening first raw file '%s' (directio=%d)\n", fname, directio);
            // Create the output directory if needed
            char datadir[1024];
            strncpy(datadir, pf.basefilename, 1023);
            char *last_slash = strrchr(datadir, '/');
            if (last_slash!=NULL && last_slash!=datadir) {
                *last_slash = '\0';
                printf("Using directory '%s' for output.\n", datadir);
		if(mkdir_p(datadir, 0755) == -1) {
		  hashpipe_error(thread_name, "mkdir_p(%s)", datadir);
		  break;
		}
            }
            // TODO: check for file exist.
            open_flags = O_CREAT|O_RDWR;
            if(directio) {
              open_flags |= O_DIRECT;
            }
            fdraw = open(fname, open_flags, 0644);
            if (fdraw==-1) {
                hashpipe_error(thread_name, "Error opening file.");
                pthread_exit(NULL);
            }

        }

        /* See if we need to open next file */
        if (block_count >= blocks_per_file) {
            close(fdraw);
            filenum++;
            char fname[256];
            sprintf(fname, "%s.%4.4d.raw", pf.basefilename, filenum);
            directio = hpguppi_read_directio_mode(ptr);
            open_flags = O_CREAT|O_RDWR;
            if(directio) {
              open_flags |= O_DIRECT;
            }
            fprintf(stderr, "Opening next raw file '%s' (directio=%d)\n", fname, directio);
            fdraw = open(fname, open_flags, 0644);
            if (fdraw==-1) {
                hashpipe_error(thread_name, "Error opening file.");
                pthread_exit(NULL);
            }
            block_count=0;
        }

        /* See how full databuf is */
        //total_status = hpguppi_input_databuf_total_status(db);

        /* Get full data block size */
        hgeti4(ptr, "BLOCSIZE", &blocksize);

        /* If we got packet 0, write data to disk */
        if (got_packet_0) {

            /* Note writing status */
            hashpipe_status_lock_safe(st);
            hputs(st->buf, status_key, "writing");
            hashpipe_status_unlock_safe(st);

            /* Write header to file */
            hend = ksearch(ptr, "END");
            len = (hend-ptr)+80;

            // If BACKEND record is not present, insert it as first record.
            // TODO: Verify that we have room to insert the record.
            if(!ksearch(ptr, "BACKEND")) {
                // Move exsiting records to make room for new first record
                memmove(ptr+80, ptr, len);
                // Copy in BACKEND_RECORD string
                strncpy(ptr, BACKEND_RECORD, 80);
                // Increase len by 80 to account for the added record
                len += 80;
            }

            // Adjust length for any padding required for DirectIO
            if(directio) {
                // Round up to next multiple of 512
                len = (len+511) & ~511;
            }

            /* Write header (and padding, if any) */
            rv = write_all(fdraw, ptr, len);
            if (rv != len) {
                char msg[100];
                perror(thread_name);
                sprintf(msg, "Error writing header (ptr=%p, len=%d, rv=%d)", ptr, len, rv);
                hashpipe_error(thread_name, msg);
		hashpipe_warn(thread_name, "closing output file and exiting");
		close(fdraw);
		// Do we need to mark block free even when we are exiting?
		break;
            }

            /* Write data */
            ptr = hpguppi_databuf_data(db, curblock);
            len = blocksize;
            if(directio) {
                // Round up to next multiple of 512
                len = (len+511) & ~511;
            }
            rv = write_all(fdraw, ptr, (size_t)len);
            if (rv != len) {
                char msg[100];
                perror(thread_name);
                sprintf(msg, "Error writing data (ptr=%p, len=%d, rv=%d)", ptr, len, rv);
                hashpipe_error(thread_name, msg);
		hashpipe_warn(thread_name, "closing output file and exiting");
		close(fdraw);
		// Do we need to mark block free even when we are exiting?
		break;
            }

	    if(!directio) {
	      /* flush output */
	      fsync(fdraw);
	    }

            /* Increment counter */
            block_count++;
        }

        /* Mark as free */
        hpguppi_input_databuf_set_free(db, curblock);

        /* Go to next block */
        curblock = (curblock + 1) % db->header.n_block;

        /* Check for cancel */
        pthread_testcancel();

    }

    hashpipe_info(thread_name, "exiting!");
    pthread_exit(NULL);

    pthread_cleanup_pop(0); /* Closes safe_close */
    pthread_cleanup_pop(0); /* Closes hpguppi_free_psrfits */

}

static hashpipe_thread_desc_t rawdisk_thread = {
    name: "hpguppi_rawdisk_only_thread",
    skey: "DISKSTAT",
    init: NULL,
    run:  run,
    ibuf_desc: {hpguppi_input_databuf_create},
    obuf_desc: {NULL}
};

static __attribute__((constructor)) void ctor()
{
  register_hashpipe_thread(&rawdisk_thread);
}

// vi: set ts=8 sw=2 noet :
