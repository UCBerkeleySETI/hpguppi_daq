/* hpguppi_read_raw_files.c
 *
 * Routine to read GUPPI RAW files and put them 
 * into shared memory blocks. 
 * Can specify output dir if want it different 
 * from input.
 * Author: Cherry Ng
 */
#define MAX_HDR_SIZE (256000)
#define BLOC_PER_FILE (128)

#define _GNU_SOURCE 1
#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <pthread.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/types.h>
#include <unistd.h>
#include "hashpipe.h"
#include "hpguppi_databuf.h"
#include "hpguppi_params.h"

int get_header_size(int fdin, char * header_buf, size_t len)
{
  int rv;
  int i=0;
  rv = read(fdin, header_buf, MAX_HDR_SIZE);
  if (rv == -1) {
    hashpipe_error("hpguppi_read_raw_files", "error reading file");
  } else if (rv > 0) {
    //Read header loop over the 80-byte records
    for (i=0; i<len; i += 80) {
      // If we found the "END " record
      if(!strncmp(header_buf+i, "END ", 4)) {
	// Move to just after END record
	i += 80;
	break;
      }
    }
  }
  return i;
}

int get_block_size(char * header_buf, size_t len)
{
  int i;
  char bs_str[32];
  int blocsize = 0;
  //Read header loop over the 80-byte records
  for (i=0; i<len; i += 80) {
    if(!strncmp(header_buf+i, "BLOCSIZE", 8)) {
      strncpy(bs_str,header_buf+i+16, 32);
      blocsize = strtoul(bs_str,NULL,0);
      break;
    }
  }
  return blocsize;
}
  
void set_output_path(char * header_buf, char * outdir, size_t len)
{
  int i;
  //Read header loop over the 80-byte records
  for (i=0; i<len; i += 80) {
    if(!strncmp(header_buf+i, "DATADIR", 7)) {
      hputs(header_buf, "DATADIR", outdir);
      break;
    }
  }
}

ssize_t read_fully(int fd, void * buf, size_t bytes_to_read)
{
  ssize_t bytes_read;
  ssize_t total_bytes_read = 0;

  while(bytes_to_read > 0) {
    bytes_read = read(fd, buf, bytes_to_read);
    if(bytes_read <= 0) {
      if(bytes_read == 0) {
	break;
      } else {
	return -1;
      }
    }
    buf += bytes_read;
    bytes_to_read -= bytes_read;
    total_bytes_read += bytes_read;
  }

  return total_bytes_read;
}

static void *run(hashpipe_thread_args_t * args)
{
    hpguppi_input_databuf_t *db  = (hpguppi_input_databuf_t *)args->obuf;
    hashpipe_status_t st = args->st;
    const char * status_key = args->thread_desc->skey;

    /* Main loop */
    int rv;
    int block_idx = 0;
    int block_count=0, filenum=0;
    int blocsize;
    char *ptr;

    //Filenames and paths
    char basefilename[200];
    char fname[256];
    hgets(st.buf, "BASEFILE", sizeof(basefilename), basefilename);
    char outdir[256];
    hgets(st.buf, "OUTDIR", sizeof(outdir), outdir);
    /* Init output file descriptor (-1 means no file open) */
    static int fdin = -1;
    char header_buf[MAX_HDR_SIZE];
    int open_flags = O_CREAT|O_RDWR|O_SYNC;
    int directio = 0;

    while (run_threads()) {
        hashpipe_status_lock_safe(&st);
        hputs(st.buf, status_key, "waiting");
        hputi4(st.buf, "NETBKOUT", block_idx);
        hashpipe_status_unlock_safe(&st);
        // Wait for data
        /* Wait for new block to be free, then clear it
         * if necessary and fill its header with new values.
         */
        while ((rv=hpguppi_input_databuf_wait_free(db, block_idx)) 
                != HASHPIPE_OK) {
            if (rv==HASHPIPE_TIMEOUT) {
                hashpipe_status_lock_safe(&st);
                hputs(st.buf, status_key, "blocked");
                hashpipe_status_unlock_safe(&st);
                continue;
            } else {
	        hashpipe_error(__FUNCTION__, "error waiting for free databuf");
                pthread_exit(NULL);
                break;
            }
        }

	//Read raw files
	if (fdin == -1) { //no file opened
	  sprintf(fname, "%s.%04d.raw", basefilename, filenum);
	  printf("Opening first raw file '%s'\n", fname);
	  
	  fdin = open(fname, open_flags, 0644);
	  if (fdin==-1) {
	    hashpipe_error(__FUNCTION__,"Error opening file.");
	    pthread_exit(NULL);
	  }
	}
	
	//Handling header - size, output path, directio----------------
	int headersize= get_header_size(fdin, header_buf, MAX_HDR_SIZE);
        set_output_path(header_buf, outdir, MAX_HDR_SIZE);

	char *header = hpguppi_databuf_header(db, block_idx);
        hashpipe_status_lock_safe(&st);
        hputs(st.buf, status_key, "receiving");
	memcpy(header, &header_buf, headersize);
        hashpipe_status_unlock_safe(&st);

	directio = hpguppi_read_directio_mode(header);
	// Adjust length for any padding required for DirectIO
	if(directio) {
	  // Round up to next multiple of 512
	  headersize = (headersize+511) & ~511;
	}

	//Read data--------------------------------------------------
	ptr = hpguppi_databuf_data(db, block_idx);
	lseek(fdin, headersize-MAX_HDR_SIZE, SEEK_CUR);
	blocsize = get_block_size(header_buf, MAX_HDR_SIZE);
	read_fully(fdin, ptr, blocsize);

	// Mark block as full
	hpguppi_input_databuf_set_filled(db, block_idx);

        // Setup for next block
        block_idx = (block_idx + 1) % N_INPUT_BLOCKS;
        block_count++;
	
	/* See if we need to open next file */
	if (block_count>= BLOC_PER_FILE) {
	  close(fdin);
	  filenum++;
	  sprintf(fname, "%s.%4.4d.raw", basefilename, filenum);
	  printf("Opening next raw file '%s'\n", fname);
	  fdin = open(fname, open_flags, 0644);
	  if (fdin==-1) {
	    hashpipe_error(__FUNCTION__,"Error opening file.");
	    pthread_exit(NULL);
	  }
	  block_count=0;
	}

        /* Will exit if thread has been cancelled */
        pthread_testcancel();

    }
    
    // Thread success!
    if (fdin!=-1) {
      close(fdin);
    }
    return THREAD_OK;

}

static hashpipe_thread_desc_t read_raw_file = {
    name: "read_raw_file",
    skey: "NETSTAT",
    init: NULL,
    run:  run,
    ibuf_desc: {NULL},
    obuf_desc: {hpguppi_input_databuf_create}
};

static __attribute__((constructor)) void ctor()
{
  register_hashpipe_thread(&read_raw_file);
}
