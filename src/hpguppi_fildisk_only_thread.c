/* hpguppi_fildisk_only_thread.c
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

#include "hpguppi_databuf.h"
#include "hpguppi_params.h"
#include "hpguppi_pksuwl.h"
#include "hpguppi_rawspec.h"
#include "hpguppi_util.h"

// 80 character string for the BACKEND header record.
static const char BACKEND_RECORD[] =
// 0000000000111111111122222222223333333333
// 0123456789012345678901234567890123456789
  "BACKEND = 'GUPPI   '                    " \
  "                                        ";

static int init(hashpipe_thread_args_t * args)
{
    int i;
    uint32_t Nc = 0;
    uint32_t Nbps = 8;
    rawspec_context * ctx;
    rawspec_callback_data_t * cb_data;

    hpguppi_input_databuf_t *db = (hpguppi_input_databuf_t *)args->ibuf;
    hashpipe_status_t *st = &args->st;
    const char * thread_name = args->thread_desc->name;

    hashpipe_status_lock_safe(st);
    // Get Nc from OBSNCHAN
    hgetu4(st->buf, "OBSNCHAN", &Nc);
    // Get Nbps from NBITS
    hgetu4(st->buf, "NBITS", &Nbps);
    hashpipe_status_unlock_safe(st);

    if(Nc == 0) {
      hashpipe_error(thread_name,
	  "OBSNCHAN not found in status buffer");
      return HASHPIPE_ERR_PARAM;
    }

    ctx = calloc(1, sizeof(rawspec_context));
    if(!ctx) {
      hashpipe_error(thread_name,
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
      hashpipe_error(thread_name,
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
      hashpipe_error(thread_name,
	  "unable to allocate rawspec h_blkbuf array");
      return HASHPIPE_ERR_SYS;
    }
    for(i=0; i < ctx->Nb_host; i++) {
      ctx->h_blkbufs[i] = (char *)&db->block[i].data;
    }

    // Initialize rawspec
    if(rawspec_initialize(ctx)) {
      hashpipe_error(thread_name,
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
    hashpipe_status_t *st = &args->st;
    const char * thread_name = args->thread_desc->name;
    const char * status_key = args->thread_desc->skey;

    rawspec_context * ctx = (rawspec_context *)args->user_data;
    rawspec_callback_data_t * cb_data = (rawspec_callback_data_t *)ctx->user_data;
    uint32_t rawspec_block_idx = 0;
    uint32_t rawspec_zero_block_count = 0;

    /* Read in general parameters */
    struct hpguppi_params gp;
    struct psrfits pf;
    pf.sub.dat_freqs = NULL;
    pf.sub.dat_weights = NULL;
    pf.sub.dat_offsets = NULL;
    pf.sub.dat_scales = NULL;
    pthread_cleanup_push((void *)hpguppi_free_psrfits, &pf);

    /* Set I/O priority class for this thread to "real time" */
    if(ioprio_set(IOPRIO_WHO_PROCESS, 0, IOPRIO_PRIO_VALUE(IOPRIO_CLASS_RT, 7))) {
      hashpipe_error(thread_name, "ioprio_set IOPRIO_CLASS_RT");
    }

    /* Loop */
    int64_t pktidx=0, pktstart=0, pktstop=0;
    int64_t piperblk=0, last_pktidx=0;
    int curblock=0;
    int got_packet_0=0, first=1;
    char *ptr;
    int rv = 0;
    int i;

    while (run_threads()) {

        /* Note waiting status */
        hashpipe_status_lock_safe(st);
	{
	  hputs(st->buf, status_key, "waiting");
	}
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
	    if(got_packet_0) {
		// Reset got_packet_0
		got_packet_0 = 0;

		// Stop rawspec here
		rawspec_stop(ctx);

		// Print end of recording conditions
		hashpipe_info(thread_name, "recording stopped: "
		    "pktstart %lu pktstop %lu pktidx %lu "
		    "rawspec blocks: %u total %u zero",
		    pktstart, pktstop, pktidx,
		    rawspec_block_idx, rawspec_zero_block_count);

		// Reset rawspec related variables
		rawspec_block_idx = 0;
		rawspec_zero_block_count = 0;
		last_pktidx = 0;
		piperblk = 0;
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
	    // piperblk will be 0 if PIPERBLK is not present (or it's 0)
	    piperblk = hpguppi_read_piperblk(ptr);
	    if(piperblk) {
	      hashpipe_info(thread_name, "found PIPERBLK %lu", piperblk);
	    }
	    // If found, pretend the previous block was the one expected
	    // If not found, this will set last_pktidx = pktidx
	    last_pktidx = pktidx - piperblk;

            char fname[256];
            // Create the output directory if needed
            char datadir[1024];
            strncpy(datadir, pf.basefilename, 1023);
            char *last_slash = strrchr(datadir, '/');
            if (last_slash!=NULL && last_slash!=datadir) {
                *last_slash = '\0';
                hashpipe_info(thread_name,
		    "Using directory '%s' for output", datadir);
		if(mkdir_p(datadir, 0755) == -1) {
		  hashpipe_error(thread_name, "mkdir_p(%s)", datadir);
		  break;
		}
            }

	    // Start new rawspec here, but first ensure that rawspec is stopped
	    rawspec_stop(ctx); // no-op if already stopped
	    rawspec_block_idx = 0;
	    rawspec_zero_block_count = 0;

	    // Update filterbank headers based on raw params and Nts etc.
	    update_fb_hdrs_from_raw_hdr(ctx, ptr);

	    // Open filterbank files
	    for(i=0; i<ctx->No; i++) {
	      sprintf(fname, "%s.%04d.fil", pf.basefilename, i);
	      hashpipe_info(thread_name, "Opening fil file '%s'", fname);
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
		hashpipe_error(thread_name,
		    "cannot open filterbank output file, giving up");
                pthread_exit(NULL);
	      }
	      posix_fadvise(cb_data[i].fd, 0, 0, POSIX_FADV_DONTNEED);

	      // Write filterbank header to output file
	      fb_fd_write_header(cb_data[i].fd, &cb_data[i].fb_hdr);
	    }
        }
	else if(got_packet_0 == 0) {
	    hashpipe_warn(thread_name,
		"pktstart %lu <= pktidx %lu < pktstop %lu, but sttvalid %d",
		pktstart, pktidx, pktstop, gp.stt_valid);
	}

        /* See how full databuf is */
        //total_status = hpguppi_input_databuf_total_status(db);

        /* If we got packet 0, write data to disk */
        if (got_packet_0) {

            /* Note waiting status */
            hashpipe_status_lock_safe(st);
            hputs(st->buf, status_key, "writing");
            hashpipe_status_unlock_safe(st);

	    // Update piperblk if piperblk is zero
	    // or pktidx is smaller than last_pktidx + piperblk
	    if(!piperblk || last_pktidx + piperblk > pktidx) {
	      piperblk = pktidx - last_pktidx;
	      if(piperblk) {
		hashpipe_info(thread_name, "inferring PIPERBLK %lu", piperblk);
	      }
	    }

	    // If piperblk is non-zero and pktidx larger than expected
	    if(piperblk && pktidx > last_pktidx + piperblk) {
	      hashpipe_info(thread_name,
		  "pktidx %lu last_pktidx %lu piperblk %lu",
		  pktidx, last_pktidx, piperblk);
	      hashpipe_warn(thread_name,
		  "treating %lu missing blocks as zeros",
		  (pktidx - last_pktidx - 1) / piperblk);

	      while(pktidx > last_pktidx + piperblk) {
		last_pktidx += piperblk;

		// If first block of a GPU input buffer
		if(rawspec_block_idx % ctx->Nb == 0) {
		  // Wait for work to complete
		  rawspec_wait_for_completion(ctx);
		}

		// Feed block of zeros to rawspec here
		hashpipe_info(thread_name,
		    "rawspec block %d is zeros", rawspec_block_idx);
		rawspec_zero_blocks_to_gpu(ctx, rawspec_block_idx, 1);
		// Increment GPU block index
		rawspec_block_idx++;
		rawspec_zero_block_count++;
		// If a multiple of Nb blocks have been sent, start processing
		if(rawspec_block_idx % ctx->Nb == 0) {
		  rawspec_start_processing(ctx, RAWSPEC_FORWARD_FFT);
		}
	      }
	    }

	    // Update last_pktidx
	    last_pktidx = pktidx;

	    // If first block of a GPU input buffer
	    if(rawspec_block_idx % ctx->Nb == 0) {
	      // Wait for work to complete (should return immediately if we're
	      // keeping up)
	      rawspec_wait_for_completion(ctx);
	    }

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
    pthread_cleanup_pop(0); /* Closes hpguppi_free_psrfits */

}

static hashpipe_thread_desc_t rawdisk_thread = {
    name: "hpguppi_fildisk_only_thread",
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
