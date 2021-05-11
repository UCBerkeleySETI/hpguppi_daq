/* hpguppi_fildisk_meerkat_thread.c
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

static void *run(hashpipe_thread_args_t * args)
{
    rawspec_context * ctx;
    rawspec_callback_data_t * cb_data;

    hpguppi_input_databuf_t *db = (hpguppi_input_databuf_t *)args->ibuf;
    hashpipe_status_t *st = &args->st;
    const char * thread_name = args->thread_desc->name;
    const char * status_key = args->thread_desc->skey;

    ctx = calloc(1, sizeof(rawspec_context));
    if(!ctx) {
      hashpipe_error(thread_name,
	  "unable to allocate rawspec context");
    }
    //Fixed paramters
    ctx->No = 1;  //No. output product
    ctx->Np = 2; 
    ctx->Npolout[0] = 1; // total power only
    // Let rawspec manage device block buffers
    ctx->Nb = 0;
    // Use databuf blocks as "caller-managed" rawspec host block buffers
    ctx->Nb_host = args->ibuf->n_block;
    ctx->h_blkbufs = malloc(ctx->Nb_host * sizeof(void *));
    if(!ctx->h_blkbufs) {
      hashpipe_error(thread_name,
		     "unable to allocate rawspec h_blkbuf array");
      pthread_exit(NULL);
    }
    for(int i=0; i < ctx->Nb_host; i++) {
      ctx->h_blkbufs[i] = (char *)&db->block[i].data;
    }
    
    // Init user_data to be array of callback data structures
    cb_data = calloc(ctx->No, sizeof(rawspec_callback_data_t));
    if(!cb_data) {
      hashpipe_error(thread_name,
		     "unable to allocate rawspec callback data");
    }
    // Init pre-defined filterbank headers
    for(int i=0; i<ctx->No; i++) {
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
	    
    /* Read in general parameters */
    struct hpguppi_params gp;
    struct psrfits pf;
    pf.sub.dat_freqs = NULL;
    pf.sub.dat_weights = NULL;
    pf.sub.dat_offsets = NULL;
    pf.sub.dat_scales = NULL;
    pthread_cleanup_push((void *)hpguppi_free_psrfits, &pf);

    /* Loop */
    int64_t pktidx=0, pktstart=0, pktstop=0, pktstart_last=0, pktstop_last=0;
    uint32_t rawspec_block_idx = 0;
    int curblock=0;
    int first_pass=1;
    char *ptr;
    int rv = 0;

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

        ptr = hpguppi_databuf_header(db, curblock);
        /* Read pktidx, pktstart, pktstop from header */
        hgeti8(ptr, "PKTIDX", &pktidx);
        hgeti8(ptr, "PKTSTART", &pktstart);
        hgeti8(ptr, "PKTSTOP", &pktstop);
	if (pktstart!=pktstart_last && pktstop!=pktstop_last) {
	  hashpipe_info(thread_name, "New pointing detected with pktstart=%lu pktstop=%lu", pktstart, pktstop);
	  first_pass = 1;
	}

        /* Set up data ptr for quant routines */
        pf.sub.data = (unsigned char *)hpguppi_databuf_data(db, curblock);

        if (first_pass==1) {
	    pktstart_last = pktstart;
	    pktstop_last = pktstop;
	    first_pass = 0;
            hpguppi_read_obs_params(ptr, &gp, &pf);

	    //Set up rawspec and context--------------------------------------------
	    hgetu4(ptr, "OBSNCHAN", &ctx->Nc); //Coarse channels
	    hgetu4(ptr, "NBITS", &ctx->Nbps); //Bit per sample
	    ctx->Ntpb = calc_ntime_per_block(BLOCK_DATA_SIZE, ctx->Nc); //Time sample per blk

	    //TODO: Figure out what these two should be, either hardcode it or pub/sub?
	    ctx->Nts[0] = (1<<15); //(1<<1); //FFT size, time samples required
	    ctx->Nas[0] = 10; //No. fine spectra to accum
	    printf("Ntpb %d Nts[0]=%d Nas=%d\n", ctx->Ntpb, ctx->Nts[0], ctx->Nas[0]);
	    
	    int Nblk = ctx->Nts[0]*ctx->Nas[0]/ctx->Ntpb;
	    if (Nblk == 0) Nblk = 1; //Read at least one blk
	    float sizeofblk = ctx->Nc*ctx->Ntpb*ctx->Np*ctx->Nbps*2 /1024./1024./8. ; //in MB
	    float Inputsize = Nblk*sizeofblk /1024.; //in GB 
	    printf("No. of blk required=%d size per blk=%f MB Inputsize=%.1f GB\n", Nblk, sizeofblk, Inputsize);

	    ctx->dump_callback = rawspec_dump_callback;

	    // Initialize rawspec
	    if(rawspec_initialize(ctx)) {
	      hashpipe_error(thread_name,
			     "rawspec initialization failed !!!");
	      pthread_exit(NULL);
	    } else {
	      // Copy fields from ctx to cb_data
	      for(int i=0; i<ctx->No; i++) {
		cb_data[i].h_pwrbuf = ctx->h_pwrbuf[i]; //Output power buffer
		cb_data[i].h_pwrbuf_size = ctx->h_pwrbuf_size[i];
		}
	    }

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
		  pthread_exit(NULL);
		}
            }

	    // Start new rawspec here, but first ensure that rawspec is stopped
	    rawspec_stop(ctx); // no-op if already stopped
	    rawspec_block_idx = 0;

	    // Update filterbank headers based on raw params and Nts etc.
	    update_fb_hdrs_from_raw_hdr(ctx, ptr);

	    // Open filterbank files
	    for(int i=0; i<ctx->No; i++) {
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

        if (first_pass == 0) {
            /* Note waiting status */
            hashpipe_status_lock_safe(st);
            hputs(st->buf, status_key, "writing");
            hashpipe_status_unlock_safe(st);

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
    name: "hpguppi_fildisk_meerkat_thread",
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
