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

#include "ioprio.h"

#include "hashpipe.h"
#include "rawspec.h"
#include "rawspec_fbutils.h"
#include "rawspec_rawutils.h"

#include "hpguppi_databuf.h"
#include "hpguppi_params.h"
#include "hpguppi_pksuwl.h"

// 80 character string for the BACKEND header record.
static const char BACKEND_RECORD[] =
// 0000000000111111111122222222223333333333
// 0123456789012345678901234567890123456789
  "BACKEND = 'GUPPI   '                    " \
  "                                        ";

#ifndef DEBUG_RAWSPEC_CALLBACKS
#define DEBUG_RAWSPEC_CALLBACKS (0)
#endif

typedef struct {
  int fd; // Output file descriptor or socket
  // TODO? unsigned int total_spectra;
  // TODO? unsigned int total_packets;
  // TODO? unsigned int total_bytes;
  // TODO? uint64_t total_ns;
  // TODO? double rate;
  // TODO? int debug_callback;
  // No way to tell if output_thread is valid expect via separate flag
  int output_thread_valid;
  pthread_t output_thread;
  // Copies of values in rawspec_context
  // (useful for output threads)
  float * h_pwrbuf;
  size_t h_pwrbuf_size;
  // TODO? unsigned int Nds;
  // TODO? unsigned int Nf; // Number of fine channels (== Nc*Nts[i])
  // Filterbank header
  fb_hdr_t fb_hdr;
} rawspec_callback_data_t;

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

void * rawspec_dump_file_thread_func(void *arg)
{
  rawspec_callback_data_t * cb_data = (rawspec_callback_data_t *)arg;

  /* Set I/O priority class for this thread to "best effort" */
  if(ioprio_set(IOPRIO_WHO_PROCESS, 0, IOPRIO_PRIO_VALUE(IOPRIO_CLASS_BE, 7))) {
    hashpipe_error("hpguppi_rawdisk_thread", "ioprio_set IOPRIO_CLASS_BE");
  }

  write_all(cb_data->fd, cb_data->h_pwrbuf, cb_data->h_pwrbuf_size);

  return NULL;
}

void rawspec_dump_callback(
    rawspec_context * ctx,
    int output_product,
    int callback_type)
{
  int rc;
  rawspec_callback_data_t * cb_data =
    &((rawspec_callback_data_t *)ctx->user_data)[output_product];

  if(callback_type == RAWSPEC_CALLBACK_PRE_DUMP) {
    if(cb_data->output_thread_valid) {
      // Join output thread
      if((rc=pthread_join(cb_data->output_thread, NULL))) {
        fprintf(stderr, "pthread_join: %s\n", strerror(rc));
      }
      // Flag thread as invalid
      cb_data->output_thread_valid = 0;
    }
  } else if(callback_type == RAWSPEC_CALLBACK_POST_DUMP) {
    if((rc=pthread_create(&cb_data->output_thread, NULL,
                      rawspec_dump_file_thread_func, cb_data))) {
      fprintf(stderr, "pthread_create: %s\n", strerror(rc));
    } else {
      cb_data->output_thread_valid = 1;
    }
  }
}

void rawspec_stop(rawspec_context * ctx)
{
  int i;
  rawspec_callback_data_t * cb_data =
    (rawspec_callback_data_t *)ctx->user_data;

  // Wait for GPU work to complete
  rawspec_wait_for_completion(ctx);

  // Close rawspec output files
  for(i=0; i<ctx->No; i++) {
    if(cb_data[i].fd != -1) {
      close(cb_data[i].fd);
      cb_data[i].fd = -1;
    }
  }
}

void update_fb_hdrs_from_raw_hdr(rawspec_context *ctx, const char *p_rawhdr)
{
  int i;
  rawspec_raw_hdr_t raw_hdr;
  rawspec_callback_data_t * cb_data = ctx->user_data;

  rawspec_raw_parse_header(p_rawhdr, &raw_hdr);
  hashpipe_info(__FUNCTION__,
      "beam_id = %d/%d", raw_hdr.beam_id, raw_hdr.nbeam);

  // Update filterbank headers based on raw params and Nts etc.
  for(i=0; i<ctx->No; i++) {
    // Same for all products
    cb_data[i].fb_hdr.telescope_id = fb_telescope_id(raw_hdr.telescop);
    cb_data[i].fb_hdr.src_raj = raw_hdr.ra;
    cb_data[i].fb_hdr.src_dej = raw_hdr.dec;
    cb_data[i].fb_hdr.tstart = raw_hdr.mjd;
    cb_data[i].fb_hdr.ibeam = raw_hdr.beam_id;
    cb_data[i].fb_hdr.nbeams = raw_hdr.nbeam;
    strncpy(cb_data[i].fb_hdr.source_name, raw_hdr.src_name, 80);
    cb_data[i].fb_hdr.source_name[80] = '\0';
    // Output product dependent
    cb_data[i].fb_hdr.foff = raw_hdr.obsbw/raw_hdr.obsnchan/ctx->Nts[i];
    // This computes correct fch1 for odd or even number of fine channels
    cb_data[i].fb_hdr.fch1 = raw_hdr.obsfreq
      - raw_hdr.obsbw*(raw_hdr.obsnchan-1)/(2*raw_hdr.obsnchan)
      - (ctx->Nts[i]/2) * cb_data[i].fb_hdr.foff
      ;//TODO + schan * raw_hdr.obsbw / raw_hdr.obsnchan; // Adjust for schan
    cb_data[i].fb_hdr.nchans = ctx->Nc * ctx->Nts[i];
    cb_data[i].fb_hdr.tsamp = raw_hdr.tbin * ctx->Nts[i] * ctx->Nas[i];
    // TODO az_start, za_start
  }
}

static int init(hashpipe_thread_args_t * args)
{
    int i;
    uint32_t Nc = 0;
    uint32_t Nbps = 8;
    rawspec_context * ctx;
    rawspec_callback_data_t * cb_data;

    hpguppi_input_databuf_t *db = (hpguppi_input_databuf_t *)args->ibuf;
    hashpipe_status_t st = args->st;

    hashpipe_status_lock_safe(&st);
    // Get Nc from OBSNCHAN
    hgetu4(st.buf, "OBSNCHAN", &Nc);
    // Get Nbps from NBITS
    hgetu4(st.buf, "NBITS", &Nbps);
    hashpipe_status_unlock_safe(&st);

    if(Nc == 0) {
      hashpipe_error("hpguppi_rawdisk_thread",
	  "OBSNCHAN not found in status buffer");
      return HASHPIPE_ERR_PARAM;
    }

    ctx = calloc(1, sizeof(rawspec_context));
    if(!ctx) {
      hashpipe_error("hpguppi_rawdisk_thread",
	  "unable to allocate rawspec context");
      return HASHPIPE_ERR_SYS;
    }

    // These values are defaults for typical BL filterbank products.
    // TODO Get from status buffer
    ctx->No = 3;
    ctx->Np = 2; // TODO Get from status buffer
    ctx->Nc = Nc;
    ctx->Nbps = Nbps;
    ctx->Npolout[0] = 1; // TODO Get from status buffer?
    ctx->Npolout[1] = 4; // TODO Get from status buffer?
    ctx->Npolout[2] = 4; // TODO Get from status buffer?

    if(Nbps == 8) {
      // Assume pre-PKSUWL (multibeam, other single pixel) data parameters.
      ctx->Ntpb = calc_ntime_per_block(BLOCK_DATA_SIZE, Nc);
      // Number of fine channels per coarse channel (i.e. FFT size).
      ctx->Nts[0] = (1<<20);
      ctx->Nts[1] = (1<<3);
      ctx->Nts[2] = (1<<10);
      // Number of fine spectra to accumulate per dump.
      ctx->Nas[0] = 51;
      ctx->Nas[1] = 128;
      ctx->Nas[2] = 3072;
    } else {
      // Assume PKSUWL data parameters
      ctx->Ntpb = PKSUWL_SAMPLES_PER_PKT * PKSUWL_PKTIDX_PER_BLOCK;
      // Number of fine channels per coarse channel (i.e. FFT size).
      ctx->Nts[0] = 64 * 1000 * 1000;
      ctx->Nts[1] = 256;
      ctx->Nts[2] = 64 * 1000;
      // Number of fine spectra to accumulate per dump.
      ctx->Nas[0] = 30;
      ctx->Nas[1] = 50;
      ctx->Nas[2] = 2000;
    }

    ctx->dump_callback = rawspec_dump_callback;

    // Init user_data to be array of callback data structures
    cb_data = calloc(ctx->No, sizeof(rawspec_callback_data_t));
    if(!cb_data) {
      hashpipe_error("hpguppi_rawdisk_thread",
	  "unable to allocate rawspec callback data");
      return HASHPIPE_ERR_SYS;
    }

    // Init pre-defined filterbank headers
    for(i=0; i<ctx->No; i++) {
      cb_data[i].fb_hdr.machine_id = 20;
      cb_data[i].fb_hdr.telescope_id = -1; // Unknown (updated later)
      cb_data[i].fb_hdr.data_type = 1;
      cb_data[i].fb_hdr.nbeams =  1;
      cb_data[i].fb_hdr.ibeam  =  1; // TODO Use actual beam ID for Parkes
      cb_data[i].fb_hdr.nbits  = 32;
      cb_data[i].fb_hdr.nifs   = ctx->Npolout[i];

      // Init callback file descriptors to sentinal values
      cb_data[i].fd = -1;
    }
    ctx->user_data = cb_data;

    // Let rawspec manage device block buffers
    ctx->Nb = 0;
    // Use databuf blocks as "caller-managed" rawspec host block buffers
    ctx->Nb_host = args->ibuf->n_block;
    ctx->h_blkbufs = malloc(ctx->Nb_host * sizeof(void *));
    if(!ctx->h_blkbufs) {
      hashpipe_error("hpguppi_rawdisk_thread",
	  "unable to allocate rawspec h_blkbuf array");
      return HASHPIPE_ERR_SYS;
    }
    for(i=0; i < ctx->Nb_host; i++) {
      ctx->h_blkbufs[i] = (char *)&db->block[i].data;
    }

    // Initialize rawspec
    if(rawspec_initialize(ctx)) {
      hashpipe_error("hpguppi_rawdisk_thread",
	  "rawspec initialization failed");
      return HASHPIPE_ERR_SYS;
    } else {
      // Copy fields from ctx to cb_data
      for(i=0; i<ctx->No; i++) {
	cb_data[i].h_pwrbuf = ctx->h_pwrbuf[i];
	cb_data[i].h_pwrbuf_size = ctx->h_pwrbuf_size[i];
	//TODO? cb_data[i].Nds = ctx->Nds[i];
	//TODO? cb_data[i].Nf  = ctx->Nts[i] * ctx->Nc;
	//TODO? cb_data[i].debug_callback = DEBUG_RAWSPEC_CALLBACKS;
      }
    }

    // Save context
    args->user_data = ctx;

    return HASHPIPE_OK;
}

static void *run(hashpipe_thread_args_t * args)
{
    // Local aliases to shorten access to args fields
    // Our output buffer happens to be a hpguppi_input_databuf
    hpguppi_input_databuf_t *db = (hpguppi_input_databuf_t *)args->ibuf;
    hashpipe_status_t st = args->st;
    const char * status_key = args->thread_desc->skey;

    rawspec_context * ctx = (rawspec_context *)args->user_data;
    rawspec_callback_data_t * cb_data = (rawspec_callback_data_t *)ctx->user_data;
    uint32_t rawspec_block_idx = 0;

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
      hashpipe_error("hpguppi_rawdisk_thread", "ioprio_set IOPRIO_CLASS_RT");
    }

    /* Loop */
    int64_t packetidx=0, pktstart=0, pktstop=0;
    int npacket=0, ndrop=0, packetsize=0, blocksize=0, len=0;
    int curblock=0;
    int block_count=0, blocks_per_file=128, filenum=0;
    int got_packet_0=0, first=1;
    char *ptr, *hend;
    int open_flags = 0;
    int directio = 0;
    int rv = 0;
    int i;

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

		// Stop rawspec here
		rawspec_stop(ctx);
		rawspec_block_idx = 0;

		// Print end of recording conditions
		hashpipe_info("hashpipe_raw_disk_thread",
		    "recording stopped: pktstart %lu pktstop %lu pktidx %lu",
		    pktstart, pktstop, packetidx);
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

	    // Start new rawspec here, but first ensure that rawspec is stopped
	    rawspec_stop(ctx); // no-op if already stopped
	    rawspec_block_idx = 0;

	    // Update filterbank headers based on raw params and Nts etc.
	    update_fb_hdrs_from_raw_hdr(ctx, ptr);

	    // Open filterbank files
	    for(i=0; i<ctx->No; i++) {
	      sprintf(fname, "%s.%04d.fil", pf.basefilename, i);
	      fprintf(stderr, "Opening fil file '%s'\n", fname);
	      last_slash = strrchr(fname, '/');
	      if(last_slash) {
		strncpy(cb_data[i].fb_hdr.rawdatafile, last_slash+1, 80);
	      } else {
		strncpy(cb_data[i].fb_hdr.rawdatafile, fname, 80);
	      }
	      cb_data[i].fb_hdr.rawdatafile[80] = '\0';

	      cb_data[i].fd = open(fname, O_CREAT|O_WRONLY|O_TRUNC|O_SYNC, 0644);
	      if(cb_data[i].fd == -1) {
		// If we can't open this output file, we probably won't be able to
		// open any more output files, so print message and bail out.
		hashpipe_error("hpguppi_rawdisk_thread",
		    "cannot open filterbank output file, giving up");
                pthread_exit(NULL);
	      }
	      posix_fadvise(cb_data[i].fd, 0, 0, POSIX_FADV_DONTNEED);

	      // Write filterbank header to output file
	      fb_fd_write_header(cb_data[i].fd, &cb_data[i].fb_hdr);
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

	    if(!directio) {
	      /* flush output */
	      fsync(fdraw);
	    }

            /* Increment counter */
            block_count++;

	    // If first block of a GPU input buffer
	    if(rawspec_block_idx % ctx->Nb == 0) {
	      // Wait for work to complete (should return immediately if we're
	      // keeping up)
	      rawspec_wait_for_completion(ctx);
	    }

	    // TODO Feed any missing blocks first
	    // TODO Add function to rawspec to use zeros for missing blocks.
	    // TODO Add function to rawspec to optimize case of missing an
	    // entire input buffer.

	    // Feed block to rawspec here
	    rawspec_copy_blocks_to_gpu(ctx, curblock, rawspec_block_idx, 1);
	    // Increment GPU block index
	    rawspec_block_idx++;
	    // If a multiple of Nb blocks have been sent, start processing
	    if(rawspec_block_idx % ctx->Nb == 0) {
	      rawspec_start_processing(ctx, RAWSPEC_FORWARD_FFT);
	    }
        }

        /* Mark as free */
        hpguppi_input_databuf_set_free(db, curblock);

        /* Go to next block */
        curblock = (curblock + 1) % db->header.n_block;

        /* Check for cancel */
        pthread_testcancel();

    }

    pthread_exit(NULL);

    // TODO Need a rawspec cleanup call
    pthread_cleanup_pop(0); /* Closes safe_close */
    pthread_cleanup_pop(0); /* Closes hpguppi_free_psrfits */

}

static hashpipe_thread_desc_t rawdisk_thread = {
    name: "hpguppi_rawdisk_thread",
    skey: "DISKSTAT",
    init: init,
    run:  run,
    ibuf_desc: {hpguppi_input_databuf_create},
    obuf_desc: {NULL}
};

static __attribute__((constructor)) void ctor()
{
  register_hashpipe_thread(&rawdisk_thread);
}

// vi: set ts=8 sw=2 noet :
