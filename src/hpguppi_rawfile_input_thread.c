/* hpguppi_rawfile_input_thread.c
 *
 * Routine to read GUPPI RAW files and put them 
 * into shared memory blocks. 
 * Can specify output dir if want it different 
 * from input.
 * Author: Cherry Ng
 */
#define MAX_HDR_SIZE (256000)
#define MAX_BLKS_PER_FILE (128)

#define _GNU_SOURCE 1
#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <pthread.h>
#include <time.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/types.h>
#include <unistd.h>
#include "hashpipe.h"
#include "hpguppi_databuf.h"
#include "hpguppi_params.h"

#include "coherent_beamformer_char_in.h"

#define VERBOSE 0
#define TIMING 0

int get_header_size(int fdin, char * header_buf, size_t len)
{
    int rv;
    int i=0;
    rv = read(fdin, header_buf, MAX_HDR_SIZE);
    if (rv == -1) {
        hashpipe_error("hpguppi_rawfile_input_thread", "error reading file");
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

    char cur_fname[200] = {0};
    char tmp_fname[200] = {0};
    strcpy(tmp_fname, "tmp_fname"); // Initialize as different string that cur_fname
    char *base_pos;
    long int period_pos;

    char outdir[256];
    hgets(st.buf, "OUTDIR", sizeof(outdir), outdir);
    /* Init output file descriptor (-1 means no file open) */
    static int fdin = -1;
    char header_buf[MAX_HDR_SIZE];
    int open_flags = O_RDONLY;
    int directio = 0;
    int sim_flag = 0; // Set to 1 if you'd like to use simulated data rather than the payload from the RAW file
    char * sim_data; // Initialize simulated data array
    int n_chan = 64; // Value set in coherent_beamformer_char_in.h and used with simulated data when no RAW file is read
    int nt = 8192; 
    sim_data = (char *)simulate_data(n_chan, nt); // Generate block of simulated data
    ssize_t read_blocsize;
    long int raw_size = 0;
    long int blocks_per_file = 0;
#if TIMING
    float read_time = 0;
    float time_taken_r = 0;
#endif

    while (run_threads()) {
        hashpipe_status_lock_safe(&st);
        hputs(st.buf, status_key, "waiting");
        hputi4(st.buf, "NETBKOUT", block_idx);
        hashpipe_status_unlock_safe(&st);
        // Wait for data
        /* Wait for new block to be free, then clear it
         * if necessary and fill its header with new values.
         */
#if VERBOSE
	printf("RAW INPUT: while ((rv=hpguppi_input_databuf_wait_free(db, block_idx)) \n");
#endif
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

#if VERBOSE
	printf("RAW INPUT: Before file open if{} \n");
#endif
        //Read raw files
        if (fdin == -1) { //no file opened
            
            // If there is no file ready at the beginning of processing then wait for it to be written to the buffer.
            // Aware of the fact that "while(True) with a break under an if statement" is considered bad practice, but 
            // it makes the most sense waiting for an arbitrary file to show up. 
            while(1){
                hashpipe_status_lock_safe(&st);
                hgets(st.buf, "RAWFILE", sizeof(cur_fname), cur_fname);
                hashpipe_status_unlock_safe(&st);
                // If a '...0000.raw' file exists, that is different from the previous '...0000.raw' file
                if ((strlen(cur_fname) != 0) && (tmp_fname != cur_fname)){
                    strcpy(tmp_fname, cur_fname); // Save this file name for comparison on the next iteration
                    printf("Got current RAW file: %s\n", cur_fname);
                    base_pos = strchr(cur_fname, '.'); // Finds the first occurence of a period in the filename
                    period_pos = base_pos-cur_fname;
                    printf("The last position of . is %ld \n", period_pos);
                    memcpy(basefilename, cur_fname, period_pos); // Copy base filename portion of file name to tmp_basefilename variable
                    hputs(st.buf, "BASEFILE", basefilename);
                    printf("RAW INPUT: Base filename from command: %s \n", basefilename);
                    break;
                }
            }
            
            sprintf(fname, "%s.%04d.raw", basefilename, filenum);
            
            printf("RAW INPUT: Opening first raw file '%s'\n", fname);
            fdin = open(fname, open_flags, 0644);
            if (fdin==-1) {
                hashpipe_error(__FUNCTION__,"Error opening file.");
                pthread_exit(NULL);
            }
            // Get size of file to help determine the number of blocks
            raw_size = lseek(fdin,0,SEEK_END); // Find the end position of file
            lseek(fdin,0,SEEK_SET); // Reset to the start
        }

        //Handling header - size, output path, directio----------------
#if VERBOSE
        printf("RAW INPUT: int headersize= get_header_size(fdin, header_buf, MAX_HDR_SIZE); \n");
#endif
        int headersize= get_header_size(fdin, header_buf, MAX_HDR_SIZE);
        set_output_path(header_buf, outdir, MAX_HDR_SIZE);

#if VERBOSE
	printf("RAW INPUT: char *header = hpguppi_databuf_header(db, block_idx); \n");
#endif
        char *header = hpguppi_databuf_header(db, block_idx);
        hashpipe_status_lock_safe(&st);
        hputs(st.buf, status_key, "receiving");
        memcpy(header, &header_buf, headersize);
        hashpipe_status_unlock_safe(&st);

	//printf("RAW INPUT: directio = hpguppi_read_directio_mode(header); \n");
        directio = hpguppi_read_directio_mode(header);

        // Adjust length for any padding required for DirectIO
        if(directio) {
            // Round up to next multiple of 512
            headersize = (headersize+511) & ~511;
        }

        //Read data--------------------------------------------------
	// Start timing read
#if TIMING
        struct timespec tval_before, tval_after;
        clock_gettime(CLOCK_MONOTONIC, &tval_before);
#endif
        ptr = hpguppi_databuf_data(db, block_idx);
        lseek(fdin, headersize-MAX_HDR_SIZE, SEEK_CUR);
        blocsize = get_block_size(header_buf, MAX_HDR_SIZE);
        if(block_count == 0){
            blocks_per_file = raw_size/(blocsize+headersize); // Number of blocks in the current RAW file
            printf("RAW INPUT: Block 0 RAW file size: %ld, and  n. blocks: %ld \n", raw_size, raw_size/(blocsize+headersize));
        }
#if VERBOSE
        printf("RAW INPUT: Block size: %d, and  BLOCK_DATA_SIZE: %d \n", blocsize, BLOCK_DATA_SIZE);
        printf("RAW INPUT: header size: %d, and  MAX_HDR_SIZE: %d \n", headersize, MAX_HDR_SIZE);
        if(block_count == 0){
            printf("RAW INPUT: Block 0 block size: %d, and  BLOCK_DATA_SIZE: %d \n", blocsize, BLOCK_DATA_SIZE);
            printf("RAW INPUT: Block 0 header size: %d, and  MAX_HDR_SIZE: %d \n", headersize, MAX_HDR_SIZE);
            printf("RAW INPUT: Block 0 RAW file size: %ld, and  n. blocks: %ld \n", raw_size, raw_size/(blocsize+headersize));
        }
#endif
	if(sim_flag == 0){
            // If a block is missing, copy a block of zeros to the buffer in it's place
            // Otherwise, write the data from a block to the buffer

	    read_blocsize = read(fdin, ptr, blocsize);
            if(block_count == 0){
                printf("RAW INPUT: Number of bytes read in read(): %zd \n", read_blocsize);
            }
#if VERBOSE
            printf("RAW INPUT: First element of buffer: %d \n", ptr[0]);
#endif
        } else{
	    memcpy(ptr, sim_data, N_INPUT);
	    //ptr += N_INPUT;
	}
#if TIMING
	// Stop timing read
        clock_gettime(CLOCK_MONOTONIC, &tval_after);
        time_taken_r = (float)(tval_after.tv_sec - tval_before.tv_sec); //*1e6; // Time in seconds since epoch
        time_taken_r = time_taken_r + (float)(tval_after.tv_nsec - tval_before.tv_nsec)*1e-9; //*1e-6; // Time in nanoseconds since 'tv_sec - start and end'
        read_time = time_taken_r;

        printf("RAW INPUT: Time taken to read from RAW file = %f ms \n", read_time);
#endif

	//printf("RAW INPUT: hpguppi_input_databuf_set_filled(db, block_idx); \n");
        // Mark block as full
        hpguppi_input_databuf_set_filled(db, block_idx);

        // Setup for next block
        block_idx = (block_idx + 1) % N_INPUT_BLOCKS;
        block_count++;

        /* See if we need to open next file */
        if (block_count>= blocks_per_file) {
            close(fdin);
            filenum++;
            // Find new basefilename after reading all the ones in the NVMe buffer
            if(filenum >= N_FILE){
                filenum=0;
                fdin=-1;
            }else{
                sprintf(fname, "%s.%4.4d.raw", basefilename, filenum);
                printf("RAW INPUT: Opening next raw file '%s'\n", fname);
                fdin = open(fname, open_flags, 0644);
                if (fdin==-1) {
                    hashpipe_error(__FUNCTION__,"Error opening file.");
                    //[TODO] Add extra blk with pktidx=0 to trigger stop.
                    pthread_exit(NULL);
                }
            }
            block_count=0;
        }

        /* Will exit if thread has been cancelled */
        pthread_testcancel();

    }

    // Thread success!
    if (fdin!=-1) {
	printf("RAW INPUT: Closing file! \n");
        close(fdin);
    }
    return THREAD_OK;

}

static hashpipe_thread_desc_t hpguppi_rawfile_input_thread = {
    name: "hpguppi_rawfile_input_thread",
    skey: "NETSTAT",
    init: NULL,
    run:  run,
    ibuf_desc: {NULL},
    obuf_desc: {hpguppi_input_databuf_create}
};

static __attribute__((constructor)) void ctor()
{
    register_hashpipe_thread(&hpguppi_rawfile_input_thread);
}
