/* hpguppi_coherent_bf_thread_fb.c
 *
 * Perform coherent beamforming and write databuf blocks out to filterbank files on disk.
 */

#define _GNU_SOURCE 1
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <assert.h>
#include <stdint.h>
#include <endian.h>
#include <math.h>

#include <unistd.h>
#include <string.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <time.h>
#include <sys/time.h>
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

static int safe_close_all(int *pfd) {
  if (pfd==NULL) return 0;
  int pfd_size = sizeof(pfd)/sizeof(int);
  int fd_val = -1;
  for(int i = 0; i < pfd_size; i++){
     if(pfd[i] == -1){
       fsync(pfd[i]);
       fd_val = close(pfd[i]);
       if(fd_val == -1){
         printf("A file was not successfully closed! \n");
       }
     }
  }
  return 0;
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
  static int fdraw[N_BEAM];
  memset(fdraw, -1, N_BEAM*sizeof(int));
  pthread_cleanup_push((void *)safe_close_all, fdraw);

  // Set I/O priority class for this thread to "real time" 
  if(ioprio_set(IOPRIO_WHO_PROCESS, 0, IOPRIO_PRIO_VALUE(IOPRIO_CLASS_RT, 7))) {
    hashpipe_error(thread_name, "ioprio_set IOPRIO_CLASS_RT");
  }

  printf("CBF: After ioprio_set...\n");

  /* Loop */
  int64_t pktidx=0, pktstart=0, pktstop=0;
  int blocksize=(N_BF_POW/N_BEAM)*sizeof(float); // Size of beamformer output
  int curblock=0;
  int block_count=0, blocks_per_file=128, filenum=0, next_filenum=0;
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


  // Timing variables
  float bf_time = 0;
  float write_time = 0;
  float time_taken = 0;
  float time_taken_w = 0;


  // --------------------- Initial delay calculations with katpoint and mosaic --------------------------------//
  // Or all calculations may be done in the while loop below
  // File descriptor
  int fdr;
  int fdw;

  // Return value of read() function
  int read_val;

  // Return value of write() function
  int write_val;

  // Array of floats to place data read from file
  float delay_pols[N_DELAYS];
  
  printf("CBF: After variable and pointer initializations ...\n");

  // FIFO file path
  char * myfifo = "/tmp/katpoint_delays";
  char * myfifo1= "/tmp/epoch";

  // Creating the named file(FIFO)
  // mkfifo(<pathname>,<permission>)
  //mkfifo(myfifo, 0666);
  

  printf("CBF: After mkfifos...\n");

/*
  // Open file as a read only
  fdr = open(myfifo,O_RDONLY);

  printf("CBF: After first open(myfifo)...\n");

  // Read file
  read_val = read(fdr, delay_pols, sizeof(delay_pols));
  if(read_val != 0){
    printf("\n");
  }

  printf("CBF: After first read()...\n");

  // Close file
  close(fdr);

  printf("Size of delay array %lu\n", sizeof(delay_pols));

  // First beam
  printf("--------------First beam delay offset---------------\n");
  printf("CBF: idx %lu in result array = %e \n", delay_idx(0, 0, 0), delay_pols[delay_idx(0, 0, 0)]);
  printf("CBF: idx %lu in result array = %e \n", delay_idx(0, 1, 0), delay_pols[delay_idx(0, 1, 0)]);
  printf("CBF: idx %lu in result array = %e \n", delay_idx(0, 2, 0), delay_pols[delay_idx(0, 2, 0)]);
  // Second beam delay
  printf("--------------Second beam delay offset--------------\n");
  printf("CBF: idx %lu in result array = %e \n", delay_idx(0, 0, 1), delay_pols[delay_idx(0, 0, 1)]);
  printf("CBF: idx %lu in result array = %e \n", delay_idx(0, 1, 1), delay_pols[delay_idx(0, 1, 1)]);
  printf("CBF: idx %lu in result array = %e \n", delay_idx(0, 2, 1), delay_pols[delay_idx(0, 2, 1)]);
  // Second beam rate
  printf("---------------Second beam delay rate----------------\n");
  printf("CBF: idx %lu in result array = %e \n", delay_idx(1, 0, 1), delay_pols[delay_idx(1, 0, 1)]); // 129
  printf("CBF: idx %lu in result array = %e \n", delay_idx(1, 1, 1), delay_pols[delay_idx(1, 1, 1)]); // 131
  printf("CBF: idx %lu in result array = %e \n", delay_idx(1, 2, 1), delay_pols[delay_idx(1, 2, 1)]); // 133
*/
  // ------------------------------------------------------------------------------------------------//

  
  uint64_t synctime = 0;
  uint64_t hclocks = 1;
  uint32_t fenchan = 1;
  double chan_bw = 1.0;
  double realtime_secs = 0.0; // Real-time in seconds according to the RAW file metadata
  double epoch_sec = 0.0; // Epoch used for delay polynomial
  int n_update_blks = 3; // Number of blocks to update coefficients with new epoch

  float* bf_coefficients; // Beamformer coefficients
  float* tmp_coefficients; // Temporary coefficients

  int sim_flag = 0; // Flag to use simulated coefficients (set to 1) or calculated beamformer coefficients (set to 0)
  // Add if statement for generate_coefficients() function option which has 3 arguments - tau, coarse frequency channel, and epoch
  if(sim_flag == 1){
    // Generate weights or coefficients (using simulate_coefficients() for now)
    tmp_coefficients = simulate_coefficients();
    // Register the array in pinned memory to speed HtoD mem copy
    coeff_pin(tmp_coefficients);
  }
  if(sim_flag == 0){
    bf_coefficients = (float*)calloc(N_COEFF, sizeof(float)); // Beamformer coefficients
    coeff_pin(bf_coefficients);
  }

  // -----------------Get phase solutions----------------- //

  // Make all initializations before while loop
  // Initialize beamformer (allocate all memory on the device)
  printf("CBF: Initializing beamformer...\n");
  init_beamformer();

  // Initialize output data array
  float* output_data;

  printf("CBF: Using host arrays allocated in pinned memory\n\r");
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

    if(filenum == next_filenum){
      next_filenum++;
      hgetu8(ptr, "SYNCTIME", &synctime);
      hgetu8(ptr, "HCLOCKS", &hclocks);
      hgetu4(ptr, "FENCHAN", &fenchan);
      hgetr8(ptr, "CHAN_BW", &chan_bw);
      hgetr8(ptr, "OBSFREQ", &obsfreq);
    }

    if(sim_flag == 0){
      // Update coefficients every specified number of blocks
      if(block_count%n_update_blks == 0){

        printf("CBF: synctime: %lu\n", synctime);
        printf("CBF: hclocks: %lu\n", hclocks);
        printf("CBF: fenchan: %lu\n", (unsigned long)fenchan);
        printf("CBF: chan_bw: %lf\n", chan_bw);
        printf("CBF: pktidx: %ld\n", pktidx);
        printf("CBF: obsfreq (MHz): %lf\n", obsfreq);

        // Calc real-time seconds since SYNCTIME for pktidx:
        //
        //                        pktidx * hclocks
        //     realtime_secs = -----------------------
        //                      2e6 * fenchan * chan_bw
        // This is the value that will be used in the delay polynomial (t, the epoch)
        if(fenchan * chan_bw != 0.0) {
          realtime_secs = (pktidx * hclocks) / (2e6 * fenchan * fabs(chan_bw));
        }

        epoch_sec = synctime + rint(realtime_secs);

        printf("CBF: Before open(myfifo1), epoch_sec = %lf...\n", epoch_sec);
  
        // Write to new FIFO going to get_delays module for delay polynomial calculation
        // Creating the named file(FIFO)
        // mkfifo(<pathname>,<permission>)
        mkfifo(myfifo1, 0666);

        while(access(myfifo1, F_OK) == 0){
          // If the FIFO has been deleted/removed by the get_delays module,
          // then that means it has been read and we can go retrieve the delay polynomials
          if(access(myfifo1, F_OK) != 0){
            printf("CBF: FIFO used to write epoch for get_delays module was deleted...\n");
            break;
          }
          // Open file as a read/write
          fdw = open(myfifo1,O_RDWR);
          if(fdw == -1){
            printf("CBF: Unable to open fifo1 file...\n");
          }

          //printf("CBF: After open(myfifo1)...\n");

          // Write to file
          write_val = write(fdw, &epoch_sec, sizeof(epoch_sec));
          if(write_val != sizeof(epoch_sec)){
            printf("CBF: Error writing to FIFO!\n");
          }

          //printf("CBF: After write(myfifo1)...\n");

          // Close file
          close(fdw);
        }

        // Wait for this FIFO to be created by get_delays module
        while(access(myfifo, F_OK) != 0){
          if(access(myfifo, F_OK) == 0){
            printf("CBF: FIFO for getting delay polynomials was created...\n");
            break;
          }
        }

        // Now get the newly calculated polynomials from FIFO written to by get_delays module
        /* Periodically get delay polynomials */
        // First check to see whether the file in /tmp used as a FIFO exists, currently called katpoint_delays and wait until it's deleted
        // If it exists, read the delays from the FIFO then compute new beamformer coefficients -> while( access( fname, F_OK ) == 0 )
        // If it doesn't exist then that means no new delays have been calculated so continue on with the previously calculated delays.
        while(access(myfifo, F_OK) == 0){
          // If the FIFO has been deleted/removed by the get_delays module,
          // then that means it has been read and we can now generate the coefficients
          if(access(myfifo, F_OK) != 0){
            printf("CBF: FIFO for getting delay polynomials was deleted...\n");
            break;
          }

          // Open file as a read only
          printf("CBF: Before open(myfifo)...\n");
          fdr = open(myfifo,O_RDWR);
          if(fdr == -1){
            printf("CBF: Unable to open fifo1 file...\n");
          }

          printf("CBF: After open(myfifo)...\n");

          // Read file
          read_val = read(fdr, delay_pols, sizeof(delay_pols));
          if(read_val != 0){
            printf("\n");
          }
          printf("CBF: After delay read()...\n");

          // Close file
          close(fdr);
        }
      }
      // Update coefficients with realtime_secs
      // Assign values to tmp variable then copy values from it to pinned memory pointer (bf_coefficients)
      tmp_coefficients = generate_coefficients(delay_pols, obsfreq, epoch_sec);
      memcpy(bf_coefficients, tmp_coefficients, N_COEFF*sizeof(float));
    }

    // If packet idx is NOT within start/stop range
    if(pktidx < pktstart || pktstop <= pktidx) {
      printf("CBF: Before checking whether files are open \n");
      for(int b = 0; b < N_BEAM; b++){
	// If file open, close it
	if(fdraw[b] != -1) {
	  // Close file
	  close(fdraw[b]);
	  // Reset fdraw, got_packet_0, filenum, block_count
	  fdraw[b] = -1;
	  if(b == 0){ // These variables only need to be set to zero once
	    got_packet_0 = 0;
	    //filenum = 0;
	    block_count=0;
	    // Print end of recording conditions
	    hashpipe_info(thread_name, "recording stopped: "
	      "pktstart %lu pktstop %lu pktidx %lu",
	      pktstart, pktstop, pktidx);
	  }
	}
      }
      printf("CBF: Before marking as free \n");
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
      printf("CBF: update_fb_hdrs_from_raw_hdr_cbf(fb_hdr, ptr) \n");
      update_fb_hdrs_from_raw_hdr_cbf(fb_hdr, ptr);

      hgetr8(ptr, "OBSBW", &obsbw);
      hgetr8(ptr,"TBIN", &tbin);
      

      // Open N_BEAM filterbank files to save a beam per file i.e. N_BIN*N_TIME*sizeof(float) per file.
      for(int b = 0; b < N_BEAM; b++){
        if(b >= 0 && b < 10) {
	  sprintf(fname, "%s.%04d-cbf0%d.fil", pf.basefilename, filenum, b);
        }else{
          sprintf(fname, "%s.%04d-cbf%d.fil", pf.basefilename, filenum, b);
        }
        hashpipe_info(thread_name, "Opening fil file '%s'", fname);
	  last_slash = strrchr(fname, '/');
        if(last_slash) {
	  strncpy(fb_hdr.rawdatafile, last_slash+1, 80);
        } else {
	  strncpy(fb_hdr.rawdatafile, fname, 80);
        }
        fb_hdr.rawdatafile[80] = '\0';
                
                
        fdraw[b] = open(fname, O_CREAT|O_WRONLY|O_TRUNC|O_SYNC, 0644);
        if(fdraw[b] == -1) {
	  // If we can't open this output file, we probably won't be able to
          // open any more output files, so print message and bail out.
          hashpipe_error(thread_name,
	    "cannot open filterbank output file, giving up");
            pthread_exit(NULL);
        }
    	posix_fadvise(fdraw[b], 0, 0, POSIX_FADV_DONTNEED);
      }


      //Fix some header stuff here due to multi-antennas
      // Need to understand and appropriately modify these values if necessary
      // Default values for now
      fb_hdr.foff = obsbw;
      fb_hdr.nchans = N_COARSE_FREQ;
      fb_hdr.fch1 = obsfreq;
      fb_hdr.nbeams = N_BEAM;
      fb_hdr.tsamp = tbin * N_TIME;
    }

    /* See if we need to open next file */
    if (block_count >= blocks_per_file) {
      filenum++;
      char fname[256];
      for(int b = 0; b < N_BEAM; b++){
	close(fdraw[b]);
        if(b >= 0 && b < 10) {
	  sprintf(fname, "%s.%04d-cbf0%d.fil", pf.basefilename, filenum, b);
        }else{
          sprintf(fname, "%s.%04d-cbf%d.fil", pf.basefilename, filenum, b);
        }
	open_flags = O_CREAT|O_RDWR|O_SYNC;
        fprintf(stderr, "CBF: Opening next fil file '%s'\n", fname);
        fdraw[b] = open(fname, open_flags, 0644);
        if (fdraw[b]==-1) {
	  hashpipe_error(thread_name, "Error opening file.");
          pthread_exit(NULL);
        }
      }
      block_count=0;
    }

    /* If we got packet 0, process and write data to disk */
    if (got_packet_0) {

      /* Note writing status */
      hashpipe_status_lock_safe(st);
      hputs(st->buf, status_key, "writing");
      hashpipe_status_unlock_safe(st);

      /* Write filterbank header to output file */
      printf("CBF: fb_fd_write_header(fdraw[b], &fb_hdr); \n");
      if(block_count == 0){
	for(int b = 0; b < N_BEAM; b++){
	  fb_hdr.ibeam =  b;
	  fb_fd_write_header(fdraw[b], &fb_hdr);
	}
      }

      printf("CBF: Before run_beamformer! \n");
      /* Write data */
      // gpu processing function here, I think...

      // Start timing beamforming computation
      struct timespec tval_before, tval_after;
      clock_gettime(CLOCK_MONOTONIC, &tval_before);

      if(sim_flag == 1){
        output_data = run_beamformer((signed char *)&db->block[curblock].data, tmp_coefficients);
      }
      if(sim_flag == 0){
        output_data = run_beamformer((signed char *)&db->block[curblock].data, bf_coefficients);
      }

      /* Set beamformer output (CUDA kernel before conversion to power), that is summing, to zero before moving on to next block*/
      set_to_zero();

      // Stop timing beamforming computation
      clock_gettime(CLOCK_MONOTONIC, &tval_after);
      time_taken = (float)(tval_after.tv_sec - tval_before.tv_sec); //*1e6; // Time in seconds since epoch
      time_taken = time_taken + (float)(tval_after.tv_nsec - tval_before.tv_nsec)*1e-9; // Time in nanoseconds since 'tv_sec - start and end'
      bf_time = time_taken;

      printf("CBF: run_beamformer() plus set_to_zero() time: %f s\n", bf_time);

      printf("CBF: First element of output data: %f\n", output_data[0]);

      // Start timing write
      struct timespec tval_before_w, tval_after_w;
      clock_gettime(CLOCK_MONOTONIC, &tval_before_w);

      // This may be okay to write to filterbank files, but I'm not entirely confident
      for(int b = 0; b < N_BEAM; b++){
	rv = write(fdraw[b], &output_data[b*N_TIME*N_FREQ], (size_t)blocksize);
	if(rv != blocksize){
	  char msg[100];
          perror(thread_name);
	  sprintf(msg, "Error writing data (output_data=%p, blocksize=%d, rv=%d)", output_data, blocksize, rv);
          hashpipe_error(thread_name, msg);
        }

	/* flush output */
	fsync(fdraw[b]);
      }

      // Stop timing write
      clock_gettime(CLOCK_MONOTONIC, &tval_after_w);
      time_taken_w = (float)(tval_after_w.tv_sec - tval_before_w.tv_sec); //*1e6; // Time in seconds since epoch
      time_taken_w = time_taken_w + (float)(tval_after_w.tv_nsec - tval_before_w.tv_nsec)*1e-9; // Time in nanoseconds since 'tv_sec - start and end'
      write_time = time_taken_w;
      printf("CBF: Time taken to write block to disk = %f s \n", write_time);
      
      printf("CBF: After write() function! Block index = %d \n", block_count);

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

  pthread_cleanup_pop(0); // Closes safe_close 

  pthread_cleanup_pop(0); /* Closes hpguppi_free_psrfits */

  // Free up device memory
  cohbfCleanup();

  hashpipe_info(thread_name, "exiting!");
  pthread_exit(NULL);
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
