/* hpguppi_rawdisk_thread.c
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

#include "hashpipe.h"

#include "hpguppi_databuf.h"
#include "hpguppi_params.h"

// 80 character string for the BACKEND header record.
static const char BACKEND_RECORD[] =
// 0000000000111111111122222222223333333333
// 0123456789012345678901234567890123456789
  "BACKEND = 'GUPPI   '                    " \
  "                                        ";

ssize_t write_all(int fd, const void *buf, size_t bytes_to_write)
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

int safe_close(int *pfd) {
    if (pfd==NULL) return 0;
    fsync(*pfd);
    return close(*pfd);
}

static void *run(hashpipe_thread_args_t * args)
{
    // Local aliases to shorten access to args fields
    // Our output buffer happens to be a hpguppi_input_databuf
    hpguppi_input_databuf_t *db = (hpguppi_input_databuf_t *)args->ibuf;
    hashpipe_status_t st = args->st;
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

    /* Loop */
    long long int packetidx=0, pktstart=0, pktstop=0;
    int npacket=0, ndrop=0, packetsize=0, blocksize=0, len=0;
    int curblock=0;
    int block_count=0, blocks_per_file=128, filenum=0;
    int got_packet_0=0, first=1;
    char *ptr, *hend;
    int open_flags = 0;
    int directio = 0;
    int rv = 0;

    while (run_threads()) {

        /* Note waiting status */
        hashpipe_status_lock_safe(&st);
        hputs(st.buf, status_key, "waiting");
        hashpipe_status_unlock_safe(&st);

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

        /* Parse packet size, npacket from header */
        hgeti8(ptr, "PKTIDX", &packetidx);
        hgeti8(ptr, "PKTSTART", &pktstart);
        hgeti8(ptr, "PKTSTOP", &pktstop);
        hgeti4(ptr, "PKTSIZE", &packetsize);
        hgeti4(ptr, "NPKT", &npacket);
        hgeti4(ptr, "NDROP", &ndrop);

	// If packet idx is NOT within start/stop range
	if(packetidx < pktstart || pktstop <= packetidx) {
	    // If file open, close it
	    if(fdraw != -1) {
		// Close file
		close(fdraw);
		// Reset fdraw, got_packet_0, filenum, block_count
		fdraw = -1;
		got_packet_0 = 0;
		filenum = 0;
		block_count=0;
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
	// it is not necessarily packetidx == 0.
        if (got_packet_0==0 && gp.stt_valid==1) {
            got_packet_0 = 1;
            hpguppi_read_obs_params(ptr, &gp, &pf);
            directio = hpguppi_read_directio_mode(ptr);
            char fname[256];
            sprintf(fname, "%s.%04d.raw", pf.basefilename, filenum);
            fprintf(stderr, "Opening raw file '%s' (directio=%d)\n", fname, directio);
            // Create the output directory if needed
            char datadir[1024];
            strncpy(datadir, pf.basefilename, 1023);
            char *last_slash = strrchr(datadir, '/');
            if (last_slash!=NULL && last_slash!=datadir) {
                *last_slash = '\0';
                printf("Using directory '%s' for output.\n", datadir);
                char cmd[1024];
                sprintf(cmd, "mkdir -m 1777 -p %s", datadir);
                rv = system(cmd);
		if(rv) {
		    if(rv == -1) {
			// system() call failed (e.g. fork() failed)
			hashpipe_error("hpguppi_rawdisk_thread", "Error calling system(\"%s\")", cmd);
		    } else {
			// system call succeeded, but command failed
			rv = WEXITSTATUS(rv); // Get exit code of command
			hashpipe_error("hpguppi_rawdisk_thread", "\"%s\" returned exit code %d (%s)",
				cmd, rv, strerror(rv));
		    }
		    pthread_exit(NULL);
		}
            }
            // TODO: check for file exist.
            open_flags = O_CREAT|O_RDWR|O_SYNC;
            if(directio) {
              open_flags |= O_DIRECT;
            }
            fdraw = open(fname, open_flags, 0644);
            if (fdraw==-1) {
                hashpipe_error("hpguppi_rawdisk_thread", "Error opening file.");
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
            open_flags = O_CREAT|O_RDWR|O_SYNC;
            if(directio) {
              open_flags |= O_DIRECT;
            }
            fprintf(stderr, "Opening raw file '%s' (directio=%d)\n", fname, directio);
            fdraw = open(fname, open_flags, 0644);
            if (fdraw==-1) {
                hashpipe_error("hpguppi_rawdisk_thread", "Error opening file.");
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

            /* Note waiting status */
            hashpipe_status_lock_safe(&st);
            hputs(st.buf, status_key, "writing");
            hashpipe_status_unlock_safe(&st);

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
                perror("hpguppi_rawdisk_thread write_all header");
                sprintf(msg, "Error writing data (ptr=%p, len=%d, rv=%d)", ptr, len, rv);
                hashpipe_error("hpguppi_rawdisk_thread", msg);
                        //"Error writing data.");
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
                perror("hpguppi_rawdisk_thread write_all block");
                sprintf(msg, "Error writing data (ptr=%p, len=%d, rv=%d)", ptr, len, rv);
                hashpipe_error("hpguppi_rawdisk_thread", msg);
                        //"Error writing data.");
            }

            /* Increment counter */
            block_count++;

            /* flush output */
            fsync(fdraw);
        }

        /* Mark as free */
        hpguppi_input_databuf_set_free(db, curblock);

        /* Go to next block */
        curblock = (curblock + 1) % db->header.n_block;

        /* Check for cancel */
        pthread_testcancel();

    }

    pthread_exit(NULL);

    pthread_cleanup_pop(0); /* Closes safe_close */
    pthread_cleanup_pop(0); /* Closes hpguppi_free_psrfits */

}

static hashpipe_thread_desc_t rawdisk_thread = {
    name: "hpguppi_rawdisk_thread",
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

// vi: set ts=8 sw=4 noet :
