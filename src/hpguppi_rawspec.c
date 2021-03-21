// hpguppi_rawspec.c
//
// Functions for using rawspec with hpguppi

#include <stdio.h>
#include <pthread.h>
#include <string.h>
#include <unistd.h>
#include <ioprio.h>

#include "hashpipe_error.h" // for hashpipe_error()
#include "hpguppi_rawspec.h"

static
ssize_t
write_all(int fd, const void *buf, size_t bytes_to_write)
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

void *
rawspec_dump_file_thread_func(void *arg)
{
  rawspec_callback_data_t * cb_data = (rawspec_callback_data_t *)arg;

  /* Set I/O priority class for this thread to "best effort" */
  if(ioprio_set(IOPRIO_WHO_PROCESS, 0, IOPRIO_PRIO_VALUE(IOPRIO_CLASS_BE, 7))) {
    hashpipe_error("hpguppi_rawdisk_thread", "ioprio_set IOPRIO_CLASS_BE");
  }

  write_all(cb_data->fd, cb_data->h_pwrbuf, cb_data->h_pwrbuf_size);

  return NULL;
}

void
rawspec_dump_callback(
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
        hashpipe_error(__FUNCTION__, "pthread_join: %s\n", strerror(rc));
      }
      // Flag thread as invalid
      cb_data->output_thread_valid = 0;
    }
  } else if(callback_type == RAWSPEC_CALLBACK_POST_DUMP) {
    if((rc=pthread_create(&cb_data->output_thread, NULL,
                      rawspec_dump_file_thread_func, cb_data))) {
      hashpipe_error(__FUNCTION__, "pthread_create: %s\n", strerror(rc));
    } else {
      cb_data->output_thread_valid = 1;
    }
  }
}

void
rawspec_stop(rawspec_context * ctx)
{
  int i;
  int rc;
  rawspec_callback_data_t * cb_data =
    (rawspec_callback_data_t *)ctx->user_data;

  // Wait for GPU work to complete
  rawspec_wait_for_completion(ctx);

  // Close rawspec output files after waiting for any worker output_threads to
  // complete.
  for(i=0; i<ctx->No; i++) {
    // If this output product's output thread is/was running
    if(cb_data[i].output_thread_valid) {
      // Join output thread
      if((rc=pthread_join(cb_data[i].output_thread, NULL))) {
        hashpipe_error(__FUNCTION__, "pthread_join: %s\n", strerror(rc));
      }
      // Flag thread as invalid
      cb_data[i].output_thread_valid = 0;
    }

    // Close output file if it was open
    if(cb_data[i].fd != -1) {
      close(cb_data[i].fd);
      cb_data[i].fd = -1;
    }
  }
}

void
update_fb_hdrs_from_raw_hdr(rawspec_context *ctx, const char *p_rawhdr)
{
  int i;
  int nbeam;
  rawspec_raw_hdr_t raw_hdr;
  rawspec_callback_data_t * cb_data = ctx->user_data;

  rawspec_raw_parse_header(p_rawhdr, &raw_hdr);
  // Default to nbeam=1 if unspecified
  nbeam = raw_hdr.nbeam == -1 ? 1 : raw_hdr.nbeam;
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
    cb_data[i].fb_hdr.nbeams = nbeam;
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
