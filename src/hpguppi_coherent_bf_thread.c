/* hpguppi_coherent_bf_thread_fb.c
 *
 * Perform coherent beamforming and write databuf blocks out to filterbank files on disk.
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

#include "coherent_beamformer_char_in.h"

#include "hashpipe.h"

// Use rawpsec_fbutils because it contains all of the necessary functions to write headers to filterbank files
// This might change in the near future to make this library completely separate from rawspec
#include "rawspec_fbutils.h"
// Use rawpsec_rawutils because it contains all of the necessary functions to parse raw headers orginally from GUPPI RAW files
// This might change in the near future to make this library completely separate from rawspec
#include "rawspec_rawutils.h"

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

//#ifndef DEBUG_RAWSPEC_CALLBACKS
//#define DEBUG_RAWSPEC_CALLBACKS (0)
//#endif

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

void
update_fb_hdrs_from_raw_hdr_cbf(fb_hdr_t fb_hdr, const char *p_rawhdr)
{
  rawspec_raw_hdr_t raw_hdr;

  rawspec_raw_parse_header(p_rawhdr, &raw_hdr);
  hashpipe_info(__FUNCTION__,
      "beam_id = %d/%d", raw_hdr.beam_id, raw_hdr.nbeam);

  // Update filterbank headers based on raw params and Nts etc.
    // Same for all products
    fb_hdr.telescope_id = fb_telescope_id(raw_hdr.telescop);
    fb_hdr.src_raj = raw_hdr.ra;
    fb_hdr.src_dej = raw_hdr.dec;
    fb_hdr.tstart = raw_hdr.mjd;
    fb_hdr.ibeam = raw_hdr.beam_id;
    fb_hdr.nbeams = raw_hdr.nbeam;
    strncpy(fb_hdr.source_name, raw_hdr.src_name, 80);
    fb_hdr.source_name[80] = '\0';
    // Output product dependent
    fb_hdr.foff = raw_hdr.obsbw/raw_hdr.obsnchan/N_TIME;
    // This computes correct fch1 for odd or even number of fine channels
    fb_hdr.fch1 = raw_hdr.obsfreq
      - raw_hdr.obsbw*(raw_hdr.obsnchan-1)/(2*raw_hdr.obsnchan)
      - (N_TIME/2) * fb_hdr.foff
      ;//TODO + schan * raw_hdr.obsbw / raw_hdr.obsnchan; // Adjust for schan
    fb_hdr.nchans = N_COARSE_FREQ;
    fb_hdr.tsamp = raw_hdr.tbin * N_TIME * 256*1024;
    // TODO az_start, za_start
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
    int blocksize=N_BF_POW*sizeof(float); // Size of beamformer output
    int curblock=0;
    int block_count=0, blocks_per_file=128, filenum=0;
    int got_packet_0=0, first=1;
    char *ptr;
    int open_flags = 0;
    int rv = 0;
    double obsbw;
    double tbin;
    double obsfreq;
    //int header_size = 0;

    // Filterbank file header initialization
    fb_hdr_t fb_hdr;
    fb_hdr.machine_id = 20;
    fb_hdr.telescope_id = -1; // Unknown (updated later)
    fb_hdr.data_type = 1;
    fb_hdr.nbeams =  1;
    fb_hdr.ibeam  =  -1; //Not used
    fb_hdr.nbits  = 32;
    fb_hdr.nifs   = 1;

    // Generate weights or coefficients (using simulate_coefficients() for now)
    float* tmp_coefficients = simulate_coefficients();
    // Register the array in pinned memory to speed HtoD mem copy
    coeff_pin(tmp_coefficients);

    // Make all initializations before while loop
    // Initialize beamformer (allocate all memory on the device)
    printf("Initializing beamformer...\n");
    init_beamformer();

    // Initialize output data array
    float* output_data;

    printf("Using host arrays allocated in pinned memory\n\r");
    for (int i = 0; i < N_INPUT_BLOCKS; i++){
        ///////////////////////////////////////////////////////////////////////////////
        //>>>>   Register host array in pinned memory <<<<
        ///////////////////////////////////////////////////////////////////////////////
        input_data_pin((signed char *)&db->block[i].data);
    }

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

	    // Update filterbank headers based on raw params and Nts etc.
	    // Possibly here
	    update_fb_hdrs_from_raw_hdr_cbf(fb_hdr, ptr);

	    hgetr8(ptr, "OBSBW", &obsbw);
	    hgetr8(ptr,"TBIN", &tbin);
	    hgetr8(ptr,"OBSFREQ", &obsfreq);

            sprintf(fname, "%s.%04d-cbf.fil", pf.basefilename, filenum);
            hashpipe_info(thread_name, "Opening fil file '%s'", fname);
            last_slash = strrchr(fname, '/');
            if(last_slash) {
                strncpy(fb_hdr.rawdatafile, last_slash+1, 80);
            } else {
                strncpy(fb_hdr.rawdatafile, fname, 80);
            }
            fb_hdr.rawdatafile[80] = '\0';

            
            fdraw = open(fname, O_CREAT|O_WRONLY|O_TRUNC|O_SYNC, 0644);
            if(fdraw == -1) {
                // If we can't open this output file, we probably won't be able to
                // open any more output files, so print message and bail out.
                hashpipe_error(thread_name,
                    "cannot open filterbank output file, giving up");
                pthread_exit(NULL);
            }
            posix_fadvise(fdraw, 0, 0, POSIX_FADV_DONTNEED);

            //Fix some header stuff here due to multi-antennas
	    // Need to understand and appropriately modify these values if necessary
	    // Default values for now
            fb_hdr.foff = obsbw;
            fb_hdr.nchans = N_COARSE_FREQ;
            fb_hdr.fch1 = obsfreq;
            fb_hdr.nbeams =  64;
            fb_hdr.tsamp = tbin * N_TIME;


/*
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
            open_flags = O_CREAT|O_RDWR|O_SYNC;
            if(directio) {
              open_flags |= O_DIRECT;
            }
            fdraw = open(fname, open_flags, 0644);
            if (fdraw==-1) {
                hashpipe_error(thread_name, "Error opening file.");
                pthread_exit(NULL);
            }
*/


        }

        /* See if we need to open next file */
        if (block_count >= blocks_per_file) {
            close(fdraw);
            filenum++;
            char fname[256];
            sprintf(fname, "%s.%04d-cbf.fil", pf.basefilename, filenum);
            open_flags = O_CREAT|O_RDWR|O_SYNC;
            fprintf(stderr, "Opening next fil file '%s'\n", fname);
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
        //hgeti4(ptr, "BLOCSIZE", &blocksize);

        /* If we got packet 0, process and write data to disk */
        if (got_packet_0) {

            /* Note writing status */
            hashpipe_status_lock_safe(st);
            hputs(st->buf, status_key, "writing");
            hashpipe_status_unlock_safe(st);

	    /* Write filterbank header to output file */
            fb_fd_write_header(fdraw, &fb_hdr);

            /* Write data */
	    // gpu processing function here, I think...
	    output_data = run_beamformer((signed char *)&db->block[curblock].data, tmp_coefficients);

	    /* Set beamformer output (CUDA kernel before conversion to power), that is summing, to zero before moving on to next block*/
	    set_to_zero();

	    printf("First element of output data: %f\n", output_data[0]);
	    printf("Last non-zero element of output data: %f\n", output_data[8388317]);
	    printf("First zero element of output data: %f\n", output_data[8388318]);
	    printf("Random element after zeros start of output data: %f\n", output_data[8400000]);
	    printf("Last element of output data: %f\n", output_data[33554431]);
	    //printf("Block size cast as size_t: %lu\n", (size_t)blocksize);

	    // This may be okay to write to filterbank files, but I'm not entirely confident
	    rv = write_all(fdraw, output_data, (size_t)blocksize);
            if (rv != blocksize) {
                char msg[100];
                perror(thread_name);
                //sprintf(msg, "Error writing data (ptr=%p, blocksize=%d, rv=%d)", ptr, blocksize, rv);
		sprintf(msg, "Error writing data (output_data=%p, blocksize=%d, rv=%d)", output_data, blocksize, rv);
                hashpipe_error(thread_name, msg);
            }

	    /* flush output */
	    fsync(fdraw);

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

    // Free up device memory
    cohbfCleanup();

    hashpipe_info(thread_name, "exiting!");
    pthread_exit(NULL);

    pthread_cleanup_pop(0); /* Closes safe_close */
    pthread_cleanup_pop(0); /* Closes hpguppi_free_psrfits */

}

static hashpipe_thread_desc_t rawdisk_thread = {
    name: "hpguppi_coherent_bf_thread",
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
