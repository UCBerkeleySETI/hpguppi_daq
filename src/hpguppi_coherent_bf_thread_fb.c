/* hpguppi_coherent_bf_thread_fb.c
 *
 * Perform coherent beamforming and write databuf blocks out to filterbank files on disk.
 */

#define _GNU_SOURCE 1
#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include <stdio.h>
#include <stdlib.h>

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

//#ifndef DEBUG_RAWSPEC_CALLBACKS
//#define DEBUG_RAWSPEC_CALLBACKS (0)
//#endif
/*
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
*/

static int safe_close(int *pfd) {
  if (pfd==NULL) return 0;
  fsync(*pfd);
  return close(*pfd);
}

//static int safe_close(int *pfd) {
//  if (pfd==NULL) return 0;
//  // Close all the files associated with each beam
//  int pfd_size = sizeof(pfd)/sizeof(int);
//  printf("Size of file descriptor array = %d \n", pfd_size);
//  for(int i = 0; i < pfd_size; i++){
//    fsync(pfd[i]);
//    return close(pfd[i]);
//  }
//}

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
  //static int fdraw = -1;
  //pthread_cleanup_push((void *)safe_close, &fdraw);

  // The pthread_cleanup_push() and pop() are used for thread safety and have to be called within the same scope i.e. { ... }
  // And all of the filterbank files associated with each beam must be closed upon termination of the threads
  // So for now, thse initializations have to be hardcoded one by one :(
  static int fdraw[N_BEAM];
  fdraw[0] = -1;
  pthread_cleanup_push((void *)safe_close, &fdraw[0]);
  fdraw[1] = -1;
  pthread_cleanup_push((void *)safe_close, &fdraw[1]);
  fdraw[2] = -1;
  pthread_cleanup_push((void *)safe_close, &fdraw[2]);
  fdraw[3] = -1;
  pthread_cleanup_push((void *)safe_close, &fdraw[3]);
  fdraw[4] = -1;
  pthread_cleanup_push((void *)safe_close, &fdraw[4]);
  fdraw[5] = -1;
  pthread_cleanup_push((void *)safe_close, &fdraw[5]);
  fdraw[6] = -1;
  pthread_cleanup_push((void *)safe_close, &fdraw[6]);
  fdraw[7] = -1;
  pthread_cleanup_push((void *)safe_close, &fdraw[7]);
  fdraw[8] = -1;
  pthread_cleanup_push((void *)safe_close, &fdraw[8]);
  fdraw[9] = -1;
  pthread_cleanup_push((void *)safe_close, &fdraw[9]);
  fdraw[10] = -1;
  pthread_cleanup_push((void *)safe_close, &fdraw[10]);
  fdraw[11] = -1;
  pthread_cleanup_push((void *)safe_close, &fdraw[11]);
  fdraw[12] = -1;
  pthread_cleanup_push((void *)safe_close, &fdraw[12]);
  fdraw[13] = -1;
  pthread_cleanup_push((void *)safe_close, &fdraw[13]);
  fdraw[14] = -1;
  pthread_cleanup_push((void *)safe_close, &fdraw[14]);
  fdraw[15] = -1;
  pthread_cleanup_push((void *)safe_close, &fdraw[15]);
  fdraw[16] = -1;
  pthread_cleanup_push((void *)safe_close, &fdraw[16]);
  fdraw[17] = -1;
  pthread_cleanup_push((void *)safe_close, &fdraw[17]);
  fdraw[18] = -1;
  pthread_cleanup_push((void *)safe_close, &fdraw[18]);
  fdraw[19] = -1;
  pthread_cleanup_push((void *)safe_close, &fdraw[19]);
  fdraw[20] = -1;
  pthread_cleanup_push((void *)safe_close, &fdraw[20]);
  fdraw[21] = -1;
  pthread_cleanup_push((void *)safe_close, &fdraw[21]);
  fdraw[22] = -1;
  pthread_cleanup_push((void *)safe_close, &fdraw[22]);
  fdraw[23] = -1;
  pthread_cleanup_push((void *)safe_close, &fdraw[23]);
  fdraw[24] = -1;
  pthread_cleanup_push((void *)safe_close, &fdraw[24]);
  fdraw[25] = -1;
  pthread_cleanup_push((void *)safe_close, &fdraw[25]);
  fdraw[26] = -1;
  pthread_cleanup_push((void *)safe_close, &fdraw[26]);
  fdraw[27] = -1;
  pthread_cleanup_push((void *)safe_close, &fdraw[27]);
  fdraw[28] = -1;
  pthread_cleanup_push((void *)safe_close, &fdraw[28]);
  fdraw[29] = -1;
  pthread_cleanup_push((void *)safe_close, &fdraw[29]);
  fdraw[30] = -1;
  pthread_cleanup_push((void *)safe_close, &fdraw[30]);
  fdraw[31] = -1;
  pthread_cleanup_push((void *)safe_close, &fdraw[31]);
  fdraw[32] = -1;
  pthread_cleanup_push((void *)safe_close, &fdraw[32]);
  fdraw[33] = -1;
  pthread_cleanup_push((void *)safe_close, &fdraw[33]);
  fdraw[34] = -1;
  pthread_cleanup_push((void *)safe_close, &fdraw[34]);
  fdraw[35] = -1;
  pthread_cleanup_push((void *)safe_close, &fdraw[35]);
  fdraw[36] = -1;
  pthread_cleanup_push((void *)safe_close, &fdraw[36]);
  fdraw[37] = -1;
  pthread_cleanup_push((void *)safe_close, &fdraw[37]);
  fdraw[38] = -1;
  pthread_cleanup_push((void *)safe_close, &fdraw[38]);
  fdraw[39] = -1;
  pthread_cleanup_push((void *)safe_close, &fdraw[39]);
  fdraw[40] = -1;
  pthread_cleanup_push((void *)safe_close, &fdraw[40]);
  fdraw[41] = -1;
  pthread_cleanup_push((void *)safe_close, &fdraw[41]);
  fdraw[42] = -1;
  pthread_cleanup_push((void *)safe_close, &fdraw[42]);
  fdraw[43] = -1;
  pthread_cleanup_push((void *)safe_close, &fdraw[43]);
  fdraw[44] = -1;
  pthread_cleanup_push((void *)safe_close, &fdraw[44]);
  fdraw[45] = -1;
  pthread_cleanup_push((void *)safe_close, &fdraw[45]);
  fdraw[46] = -1;
  pthread_cleanup_push((void *)safe_close, &fdraw[46]);
  fdraw[47] = -1;
  pthread_cleanup_push((void *)safe_close, &fdraw[47]);
  fdraw[48] = -1;
  pthread_cleanup_push((void *)safe_close, &fdraw[48]);
  fdraw[49] = -1;
  pthread_cleanup_push((void *)safe_close, &fdraw[49]);
  fdraw[50] = -1;
  pthread_cleanup_push((void *)safe_close, &fdraw[50]);
  fdraw[51] = -1;
  pthread_cleanup_push((void *)safe_close, &fdraw[51]);
  fdraw[52] = -1;
  pthread_cleanup_push((void *)safe_close, &fdraw[52]);
  fdraw[53] = -1;
  pthread_cleanup_push((void *)safe_close, &fdraw[53]);
  fdraw[54] = -1;
  pthread_cleanup_push((void *)safe_close, &fdraw[54]);
  fdraw[55] = -1;
  pthread_cleanup_push((void *)safe_close, &fdraw[55]);
  fdraw[56] = -1;
  pthread_cleanup_push((void *)safe_close, &fdraw[56]);
  fdraw[57] = -1;
  pthread_cleanup_push((void *)safe_close, &fdraw[57]);
  fdraw[58] = -1;
  pthread_cleanup_push((void *)safe_close, &fdraw[58]);
  fdraw[59] = -1;
  pthread_cleanup_push((void *)safe_close, &fdraw[59]);
  fdraw[60] = -1;
  pthread_cleanup_push((void *)safe_close, &fdraw[60]);
  fdraw[61] = -1;
  pthread_cleanup_push((void *)safe_close, &fdraw[61]);
  fdraw[62] = -1;
  pthread_cleanup_push((void *)safe_close, &fdraw[62]);
  fdraw[63] = -1;
  pthread_cleanup_push((void *)safe_close, &fdraw[63]);

  // Set I/O priority class for this thread to "real time" 
  if(ioprio_set(IOPRIO_WHO_PROCESS, 0, IOPRIO_PRIO_VALUE(IOPRIO_CLASS_RT, 7))) {
    hashpipe_error(thread_name, "ioprio_set IOPRIO_CLASS_RT");
  }

  // ------------ Prefered way to do the above, but can't given the current setup ------------------ //
  /*
  for(int b = 0; b < N_BEAM; b++)
    fdraw[b] = -1;

  for(int b = 0; b < N_BEAM; b++)
    pthread_cleanup_push((void *)safe_close, &fdraw[b]);

  // Set I/O priority class for this thread to "real time" 
  if(ioprio_set(IOPRIO_WHO_PROCESS, 0, IOPRIO_PRIO_VALUE(IOPRIO_CLASS_RT, 7))) {
    hashpipe_error(thread_name, "ioprio_set IOPRIO_CLASS_RT");
  }
  */
  // ---------------------------------------------------------------------------------------------- //

  /* Loop */
  int64_t pktidx=0, pktstart=0, pktstop=0;
  int blocksize=(N_BF_POW/N_BEAM)*sizeof(float); // Size of beamformer output
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

  // Timing variables
  float bf_time = 0;
  float write_time = 0;
  float time_taken = 0;
  float time_taken_w = 0;

  // --------------------- Calculate delays with katpoint and mosaic --------------------------------//
  float time_taken_d = 0;
  float delaycalc_time = 0;
  int arr_size = 0;
  float* result;

  PyObject* myModuleString;
  PyObject* myModule;
  PyObject* myClass;
  PyObject* arglist;
  PyObject* myInst;
  PyObject* myMethod;
  PyObject* arglist2;
  PyObject* myResult;
  PyObject* result_tmp;

  // Start timing delay calculation //
  struct timespec tval_before, tval_after;

  // Start timing delay calculation //
  clock_gettime(CLOCK_MONOTONIC, &tval_before);

  // Initialize python the python interpreter //
  Py_Initialize();

  // Import python module //
  myModuleString = PyUnicode_DecodeFSDefault("get_delays");
  assert(myModuleString != NULL);
  myModule = PyImport_Import(myModuleString);
  assert(myModule != NULL);
  Py_DECREF(myModuleString);
	
  // Get referrence to class //
  myClass = PyObject_GetAttrString(myModule, "DelayPolynomial");
  assert(myClass != NULL);
  Py_DECREF(myModule);

  int arg = 1;
  float freq_flag = 1.4e9; // This is the argument to change the flag in the python script //
  // First argument is the size of the tuple (number of arguments).
  // Second and onward arguments are the arguments to the __init__ function of the class.
  arglist = PyTuple_Pack(arg, PyFloat_FromDouble(freq_flag)); 
  assert(arglist != NULL);

  // Get class //
  myInst = PyObject_CallObject(myClass, arglist);
  assert(myInst != NULL);
	
  // Get referrence to method/function //
  myMethod  = PyObject_GetAttrString(myInst, "get_delay_polynomials"); // fetch bound method //
  assert(myMethod != NULL);
  Py_DECREF(myInst);

  int arg2 = 2;
  int time_arg = 1629380016;
  int dur_flag = 2;
  // No argument needed for the gen_some_vals //
  arglist2 = PyTuple_Pack(arg2, PyFloat_FromDouble(time_arg), PyFloat_FromDouble(dur_flag));
  assert(arglist2 != NULL);

  // Get result from function //
  myResult = PyObject_CallObject(myMethod, arglist2);
  Py_DECREF(myMethod);
  Py_DECREF(arglist2);
  assert(myResult != NULL);
	
  /*
  // Gets a single value from a variable in python //
  float result = (float)PyFloat_AsDouble(myResult);
  Py_DECREF(myResult);
  printf("Result from python module = %f \n", result);
  */

  // Gets the size of the array generated by python script //
  int arr_size = PyList_Size(myResult);
  printf("Length of array = %d \n", arr_size);

  // Gets the values from the array //
  result = (float*)calloc(arr_size, sizeof(float));
  for(int i = 0; i < arr_size; i++){
    result_tmp = PyList_GetItem(myResult, i);
    result[i] = (float)PyFloat_AsDouble(result_tmp);
  }

  // Stop timing beamforming computation //
  clock_gettime(CLOCK_MONOTONIC, &tval_after);
  time_taken_d = (float)(tval_after.tv_sec - tval_before.tv_sec); //*1e6; // Time in seconds since epoch
  time_taken_d = time_taken_d + (float)(tval_after.tv_nsec - tval_before.tv_nsec)*1e-9; // Time in nanoseconds since 'tv_sec - start and end'
  delaycalc_time = time_taken_d;

  printf("Average delay calculation time: %f s\n", delaycalc_time);

  // First beam
  printf("--------------First beam---------------\n");
  printf("idx %d in result array = %e \n", 0, result[0]);
  printf("idx %d in result array = %e \n", 1, result[1]);
  printf("idx %d in result array = %e \n", 2, result[2]);
  // Second beam delay
  printf("--------------Second beam--------------\n");
  printf("idx %d in result array = %e \n", 64*2, result[64*2]);
  printf("idx %d in result array = %e \n", (64*2)+2, result[(64*2)+2]);
  printf("idx %d in result array = %e \n", (64*2)+(2*2), result[(64*2)+(2*2)]);
  // Second beam rate
  printf("-----------Second beam rate------------\n");
  printf("idx %d in result array = %e \n", (64*2)+1, result[(64*2)+1]);
  printf("idx %d in result array = %e \n", (64*2)+2+1, result[(64*2)+2+1]);
  printf("idx %d in result array = %e \n", (64*2)+(2*2)+1, result[(64*2)+(2*2)+1]);

  Py_Finalize();
  // ------------------------------------------------------------------------------------------------//

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
      printf("Before checking whether files are open \n");
      for(int b = 0; b < N_BEAM; b++){
	// If file open, close it
	if(fdraw[b] != -1) {
	  // Close file
	  close(fdraw[b]);
	  // Reset fdraw, got_packet_0, filenum, block_count
	  fdraw[b] = -1;
	  if(b == 0){ // These variables only need to be set to zero once
	    got_packet_0 = 0;
	    filenum = 0;
	    block_count=0;
	    // Print end of recording conditions
	    hashpipe_info(thread_name, "recording stopped: "
	      "pktstart %lu pktstop %lu pktidx %lu",
	      pktstart, pktstop, pktidx);
	  }
	}
      }
      printf("Before marking as free \n");
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
      printf("update_fb_hdrs_from_raw_hdr_cbf(fb_hdr, ptr) \n");
      update_fb_hdrs_from_raw_hdr_cbf(fb_hdr, ptr);

      hgetr8(ptr, "OBSBW", &obsbw);
      hgetr8(ptr,"TBIN", &tbin);
      hgetr8(ptr,"OBSFREQ", &obsfreq);

      // Open N_BEAM filterbank files to save a beam per file i.e. N_BIN*N_TIME*sizeof(float) per file.
      for(int b = 0; b < N_BEAM; b++){
	sprintf(fname, "%s.%04d-cbf%d.fil", pf.basefilename, filenum, b);
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
      filenum++;
      char fname[256];
      for(int b = 0; b < N_BEAM; b++){
	close(fdraw[b]);
	sprintf(fname, "%s.%04d-cbf%d.fil", pf.basefilename, filenum, b);
	open_flags = O_CREAT|O_RDWR|O_SYNC;
        fprintf(stderr, "Opening next fil file '%s'\n", fname);
        fdraw[b] = open(fname, open_flags, 0644);
        if (fdraw[b]==-1) {
	  hashpipe_error(thread_name, "Error opening file.");
          pthread_exit(NULL);
        }
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
      printf("fb_fd_write_header(fdraw[b], &fb_hdr); \n");
      if(block_count == 0){
	for(int b = 0; b < N_BEAM; b++){
	  fb_hdr.ibeam =  b;
	  fb_fd_write_header(fdraw[b], &fb_hdr);
	}
      }
	    
      printf("Before run_beamformer! \n");

      /* Write data */
      // gpu processing function here, I think...

      // Start timing beamforming computation
      struct timespec tval_before, tval_after;
      clock_gettime(CLOCK_MONOTONIC, &tval_before);

      output_data = run_beamformer((signed char *)&db->block[curblock].data, tmp_coefficients);

      /* Set beamformer output (CUDA kernel before conversion to power), that is summing, to zero before moving on to next block*/
      set_to_zero();

      // Stop timing beamforming computation
      clock_gettime(CLOCK_MONOTONIC, &tval_after);
      time_taken = (float)(tval_after.tv_sec - tval_before.tv_sec); //*1e6; // Time in seconds since epoch
      time_taken = time_taken + (float)(tval_after.tv_nsec - tval_before.tv_nsec)*1e-9; // Time in nanoseconds since 'tv_sec - start and end'
      bf_time = time_taken;

      printf("run_beamformer() plus set_to_zero() time: %f s\n", bf_time);

      printf("First element of output data: %f\n", output_data[0]);
      //printf("Last non-zero element of output data: %f\n", output_data[8388317]);
      //printf("First zero element of output data: %f\n", output_data[8388318]);
      //printf("Random element after zeros start of output data: %f\n", output_data[8400000]);
      //printf("Last element of output data: %f\n", output_data[33554431]);
      //printf("Block size cast as size_t: %lu\n", (size_t)blocksize);

      // Start timing write
      struct timespec tval_before_w, tval_after_w;
      clock_gettime(CLOCK_MONOTONIC, &tval_before_w);

      // This may be okay to write to filterbank files, but I'm not entirely confident
      for(int b = 0; b < N_BEAM; b++){
	//rv = write_all(fdraw[b], &output_data[b*N_TIME*N_FREQ], (size_t)blocksize);
	rv = write(fdraw[b], &output_data[b*N_TIME*N_FREQ], (size_t)blocksize);
	if(rv != blocksize){
	  char msg[100];
          perror(thread_name);
          //sprintf(msg, "Error writing data (ptr=%p, blocksize=%d, rv=%d)", ptr, blocksize, rv);
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
      printf("Time taken to write block to disk = %f s \n", write_time);
      
      printf("After write() function! Block index = %d \n", block_count);

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

  // ------------ Prefered way to do the below calls, but can't given the current setup ------------------ //
  /*
  for(int b = 0; b < N_BEAM; b++)
    pthread_cleanup_pop(0); // Closes safe_close
  */
  // ---------------------------------------------------------------------------------------------------- //

  pthread_cleanup_pop(0); // Closes safe_close 
  pthread_cleanup_pop(0); // Closes safe_close 
  pthread_cleanup_pop(0); // Closes safe_close 
  pthread_cleanup_pop(0); // Closes safe_close 
  pthread_cleanup_pop(0); // Closes safe_close 
  pthread_cleanup_pop(0); // Closes safe_close 
  pthread_cleanup_pop(0); // Closes safe_close 
  pthread_cleanup_pop(0); // Closes safe_close 
  pthread_cleanup_pop(0); // Closes safe_close 
  pthread_cleanup_pop(0); // Closes safe_close 
  pthread_cleanup_pop(0); // Closes safe_close 
  pthread_cleanup_pop(0); // Closes safe_close 
  pthread_cleanup_pop(0); // Closes safe_close 
  pthread_cleanup_pop(0); // Closes safe_close 
  pthread_cleanup_pop(0); // Closes safe_close 
  pthread_cleanup_pop(0); // Closes safe_close 
  pthread_cleanup_pop(0); // Closes safe_close 
  pthread_cleanup_pop(0); // Closes safe_close 
  pthread_cleanup_pop(0); // Closes safe_close 
  pthread_cleanup_pop(0); // Closes safe_close 
  pthread_cleanup_pop(0); // Closes safe_close 
  pthread_cleanup_pop(0); // Closes safe_close 
  pthread_cleanup_pop(0); // Closes safe_close 
  pthread_cleanup_pop(0); // Closes safe_close 
  pthread_cleanup_pop(0); // Closes safe_close 
  pthread_cleanup_pop(0); // Closes safe_close 
  pthread_cleanup_pop(0); // Closes safe_close 
  pthread_cleanup_pop(0); // Closes safe_close 
  pthread_cleanup_pop(0); // Closes safe_close 
  pthread_cleanup_pop(0); // Closes safe_close 
  pthread_cleanup_pop(0); // Closes safe_close 
  pthread_cleanup_pop(0); // Closes safe_close 
  pthread_cleanup_pop(0); // Closes safe_close 
  pthread_cleanup_pop(0); // Closes safe_close 
  pthread_cleanup_pop(0); // Closes safe_close 
  pthread_cleanup_pop(0); // Closes safe_close 
  pthread_cleanup_pop(0); // Closes safe_close 
  pthread_cleanup_pop(0); // Closes safe_close 
  pthread_cleanup_pop(0); // Closes safe_close 
  pthread_cleanup_pop(0); // Closes safe_close 
  pthread_cleanup_pop(0); // Closes safe_close 
  pthread_cleanup_pop(0); // Closes safe_close 
  pthread_cleanup_pop(0); // Closes safe_close 
  pthread_cleanup_pop(0); // Closes safe_close 
  pthread_cleanup_pop(0); // Closes safe_close 
  pthread_cleanup_pop(0); // Closes safe_close 
  pthread_cleanup_pop(0); // Closes safe_close 
  pthread_cleanup_pop(0); // Closes safe_close 
  pthread_cleanup_pop(0); // Closes safe_close 
  pthread_cleanup_pop(0); // Closes safe_close 
  pthread_cleanup_pop(0); // Closes safe_close 
  pthread_cleanup_pop(0); // Closes safe_close 
  pthread_cleanup_pop(0); // Closes safe_close 
  pthread_cleanup_pop(0); // Closes safe_close 
  pthread_cleanup_pop(0); // Closes safe_close 
  pthread_cleanup_pop(0); // Closes safe_close 
  pthread_cleanup_pop(0); // Closes safe_close 
  pthread_cleanup_pop(0); // Closes safe_close 
  pthread_cleanup_pop(0); // Closes safe_close 
  pthread_cleanup_pop(0); // Closes safe_close 
  pthread_cleanup_pop(0); // Closes safe_close 
  pthread_cleanup_pop(0); // Closes safe_close 
  pthread_cleanup_pop(0); // Closes safe_close 
  pthread_cleanup_pop(0); // Closes safe_close 

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
