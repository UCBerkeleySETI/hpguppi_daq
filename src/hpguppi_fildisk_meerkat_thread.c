/* hpguppi_fildisk_meerkat_thread.c
 *
 * Write databuf blocks out to disk as filterbank files.
 * Forms only incoherent summed beams.
 * Input can be raw files or incoming packets.
 *
 * Author: Cherry Ng
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
    unsigned int Nant = 0; // Number of antenna (Nc is a multiple of this)
    unsigned int Nc   = 0; // Number of coarse channels
    unsigned int Ntpb = 0; // Number of time samples per block
    unsigned int Nbps = 0; // Number of bits per sample
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
    ctx->Np = 2;  //No. of pol
    ctx->Npolout[0] = 1; // total power only

    //Always do incoherent sum for now
    ctx->incoherently_sum = 1;
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
        cb_data[i].fb_hdr.ibeam  =  -1; //Not used
        cb_data[i].fb_hdr.nbits  = 32;
        cb_data[i].fb_hdr.nifs   = ctx->Npolout[i];

        // Init callback file descriptors to sentinal values
        cb_data[i].fd = -1;
        if (ctx->incoherently_sum) {
            cb_data[i].fd_ics = -1;
        }
    }

    ctx->dump_callback = rawspec_dump_callback;
    ctx->user_data = cb_data;

    /* Read in (legacy) general parameters */
    struct hpguppi_params gp;
    struct psrfits pf;
    pf.sub.dat_freqs = NULL;
    pf.sub.dat_weights = NULL;
    pf.sub.dat_offsets = NULL;
    pf.sub.dat_scales = NULL;
    pthread_cleanup_push((void *)hpguppi_free_psrfits, &pf);

    /* Loop */
    int64_t pktidx=0, pktstart=0, pktstop=0;
    int64_t piperblk=0, last_pktidx=0;
    uint32_t rawspec_block_idx = 0;
    uint32_t rawspec_zero_block_count = 0;
    int curblock=0;
    int first_pass=1;
    char *ptr;
    int rv = 0, i=0;
    double obsbw;
    double tbin;
    double obsfreq;

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

        //Check for out of range pktidx
        if (!first_pass && (pktidx < pktstart || pktidx >= pktstop)) {
            // Print end of recording conditions
            hashpipe_info(thread_name, "recording stopped: "
                "pktstart %lu pktstop %lu pktidx %lu "
                "rawspec blocks: %u total %u zero",
                pktstart, pktstop, pktidx,
                rawspec_block_idx, rawspec_zero_block_count);

            // Set flag for next recording
            first_pass = 1;
            // Stop rawspec
            rawspec_stop(ctx);
            // Reset rawspec related variables
            rawspec_reset_integration(ctx);
            rawspec_block_idx = 0;
            rawspec_zero_block_count = 0;
        }

        /* Set up data ptr for quant routines */
        pf.sub.data = (unsigned char *)hpguppi_databuf_data(db, curblock);

        if (first_pass==1 && (pktidx>pktstart && pktidx<pktstop)) {
            first_pass = 0;
            hpguppi_read_obs_params(ptr, &gp, &pf);

            //Set up rawspec and context--------------------------------------------
            hgetu4(ptr, "NANTS", &Nant);  // Number of antennas
            hgetu4(ptr, "OBSNCHAN", &Nc); // Number of coarse channels
            hgetu4(ptr, "NBITS", &Nbps);  // Bits per sample
            Ntpb = calc_ntime_per_block(BLOCK_DATA_SIZE, Nc); // Time sample per blk

            // Preserve F Engine resolution, regardless of F Engine mode (1K,4K,32K).
            ctx->Nts[0] = 1; //FFT size, time samples required
            // Accumulate 256*1024 spectra per dump.  This number was chosen
            // because it is an easy(ish) number to remmeber that is a power of
            // two (NB: need to divide ntime samples per block or vice versa!)
            // and results in modest dump rates of ~1.25 second integrations in
            // 4K mode.  1K mode gets ~0.31 second integration and 32K mode
            // gets ~10 second integrations, but all modes have the same output
            // data rate.
            ctx->Nas[0] = 256*1024; //No. fine spectra to accum
            hashpipe_info(thread_name,
                "Ntpb=%d Nts[0]=%d Nas=%d\n", Ntpb, ctx->Nts[0], ctx->Nas[0]);

#if 0
            int Nblk = ctx->Nts[0]*ctx->Nas[0]/Ntpb;
            if (Nblk == 0) Nblk = 1; //Read at least one blk
            float sizeofblk = Nc*Ntpb*ctx->Np*Nbps*2 /1024./1024./8. ; //in MB
            float Inputsize = Nblk*sizeofblk /1024.; //in GB

            hashpipe_info(thread_name,
                "No. of blk required=%d size per blk=%f MB Inputsize=%.1f GB\n", Nblk, sizeofblk, Inputsize);
#endif // 0

            // If block dimensions have changed (we assume Np and
            // input_conjugated never change)
            if(Nc != ctx->Nc || Nbps != ctx->Nbps || Ntpb != ctx->Ntpb
            || Nant != ctx->Nant) {
              // Cleanup previous setup, if initialized
              if(ctx->Ntpb != 0) {
                rawspec_cleanup(ctx);
              }
              // If number of antennas has changed
              if(Nant += ctx->Nant) {
                ctx->Naws = Nant;
                if(ctx->Aws) {
                  free(ctx->Aws);
                }
                ctx->Aws = malloc(ctx->Naws*sizeof(float));
                //Assign weights for the incoherent sum, currently just 1s
                for(int i=0; i < ctx->Naws; i++){
                    ctx->Aws[i] = 1.0f;
                }
              }

              // Remember new dimensions
              ctx->Nant = Nant;
              ctx->Nc   = Nc;
              ctx->Ntpb = Ntpb;
              ctx->Nbps = Nbps;

              // Initialize for new dimensions and/or conjugation
              ctx->Nb = 0;           // auto-calculate

              // Initialize rawspec
              if(rawspec_initialize(ctx)) {
                  hashpipe_error(thread_name,
                      "rawspec initialization failed !!!");
                  pthread_exit(NULL);
              }
            }
            // Copy fields from ctx to cb_data
            for(i=0; i<ctx->No; i++) {
                if (ctx->incoherently_sum) {
                    cb_data[i].h_pwrbuf_size = ctx->h_pwrbuf_size[i]/ctx->Nant;
                    cb_data[i].h_icsbuf = ctx->h_icsbuf[i];
                }
                else {
                    cb_data[i].h_pwrbuf_size = ctx->h_pwrbuf_size[i];
                    cb_data[i].h_pwrbuf = ctx->h_pwrbuf[i]; //Output power buffer
                }
            }

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
                    pthread_exit(NULL);
                }
            }

            // Start new rawspec here, but first ensure that rawspec is stopped
            rawspec_stop(ctx); // no-op if already stopped
            rawspec_reset_integration(ctx);
            rawspec_block_idx = 0;
            rawspec_zero_block_count = 0;

            // Update filterbank headers based on raw params and Nts etc.
            update_fb_hdrs_from_raw_hdr(ctx, ptr);

            hgetr8(ptr, "OBSBW", &obsbw);
            hgetr8(ptr,"TBIN", &tbin);
            hgetr8(ptr,"OBSFREQ", &obsfreq);

            // Open filterbank files
            for(i=0; i<ctx->No; i++) {
                if (ctx->incoherently_sum) {
                    sprintf(fname, "%s.%04d-ics.fil", pf.basefilename, i);
                } else {
                    sprintf(fname, "%s.%04d.fil", pf.basefilename, i);
                }
                hashpipe_info(thread_name, "Opening fil file '%s'", fname);
                last_slash = strrchr(fname, '/');
                if(last_slash) {
                    strncpy(cb_data[i].fb_hdr.rawdatafile, last_slash+1, 80);
                } else {
                    strncpy(cb_data[i].fb_hdr.rawdatafile, fname, 80);
                }
                cb_data[i].fb_hdr.rawdatafile[80] = '\0';

                if (ctx->incoherently_sum) {
                    cb_data[i].fd_ics = open(fname, O_CREAT|O_WRONLY|O_TRUNC|O_SYNC, 0644);
                    if(cb_data[i].fd_ics == -1) {
                        // If we can't open this output file, we probably won't be able to
                        // open any more output files, so print message and bail out.
                        hashpipe_error(thread_name,
                            "cannot open filterbank output file, giving up");
                        pthread_exit(NULL);
                    }
                    posix_fadvise(cb_data[i].fd_ics, 0, 0, POSIX_FADV_DONTNEED);
                } else {
                    cb_data[i].fd = open(fname, O_CREAT|O_WRONLY|O_TRUNC|O_SYNC, 0644);
                    if(cb_data[i].fd == -1) {
                        hashpipe_error(thread_name,
                            "cannot open filterbank output file, giving up");
                        pthread_exit(NULL);
                    }
                    posix_fadvise(cb_data[i].fd, 0, 0, POSIX_FADV_DONTNEED);
                }

                //Fix some header stuff here due to multi-antennas
                cb_data[i].fb_hdr.foff =
                    obsbw/(ctx->Nc/ctx->Nant)/ctx->Nts[i];
                cb_data[i].fb_hdr.nchans = ctx->Nc * ctx->Nts[i];
                cb_data[i].fb_hdr.fch1 = obsfreq
                    - obsbw*((ctx->Nc/ctx->Nant)-1)
                    / (2*ctx->Nc/ctx->Nant)
                    - (ctx->Nts[i]/2) * cb_data[i].fb_hdr.foff
                    + (0 % (ctx->Nc/ctx->Nant)) // Adjust for schan
                    * obsbw / (ctx->Nc/ctx->Nant);
                cb_data[i].fb_hdr.nbeams =  1;
                cb_data[i].fb_hdr.tsamp = tbin * ctx->Nts[i] * ctx->Nas[i];

                // Write filterbank header to output file
                if (ctx->incoherently_sum) {
                    cb_data[i].fb_hdr.nchans /= ctx->Nant;
                    fb_fd_write_header(cb_data[i].fd_ics, &cb_data[i].fb_hdr);
                    cb_data[i].fb_hdr.nchans *= ctx->Nant;
                } else {
                    fb_fd_write_header(cb_data[i].fd, &cb_data[i].fb_hdr);
                }
            }
        }

        if (pktidx>pktstart && pktidx<pktstop) {
            /* Note waiting status */
            hashpipe_status_lock_safe(st);
            hputs(st->buf, status_key, "writing");
            hashpipe_status_unlock_safe(st);

            // DROPPED PACKETS: If piperblk is non-zero and pktidx larger than expected
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
