// hpguppi_rawspec.h
//
// Header file for using rawspec with hpguppi

#ifndef _HPGUPPI_RAWSPEC_H_
#define _HPGUPPI_RAWSPEC_H_

#include "rawspec.h"
#include "rawspec_fbutils.h"
#include "rawspec_rawutils.h"

#ifndef DEBUG_RAWSPEC_CALLBACKS
#define DEBUG_RAWSPEC_CALLBACKS (0)
#endif

typedef struct {
  int fd; // Output file descriptor or socket
  int fd_ics;
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
  float * h_icsbuf;
  // TODO? unsigned int Nds;
  // TODO? unsigned int Nf; // Number of fine channels (== Nc*Nts[i])
  // Filterbank header
  fb_hdr_t fb_hdr;
} rawspec_callback_data_t;

// Main function of worker thread used to write rawspec output to file
void * rawspec_dump_file_thread_func(void *arg);

// Rawspec dump callback function
void rawspec_dump_callback(
    rawspec_context * ctx,
    int output_product,
    int callback_type);

// Waits for current rawspec activity to complete, then closes output files
void rawspec_stop(rawspec_context * ctx);

// Updates filterbank headers from GUPPI RAW headers
void update_fb_hdrs_from_raw_hdr(rawspec_context *ctx, const char *p_rawhdr);

#endif // _HPGUPPI_RAWSPEC_H_
