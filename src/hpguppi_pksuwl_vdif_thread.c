// hpguppi_pksuwl_vdif_thread.c
//
// A Hashpipe thread that proceeses Parkes UWL VDIF packets from an input
// buffer populated by hpguppi_ibverbs_pkt_thread and assembles them into GUPPI
// RAW blocks.

// TODO TEST Wait for first (second?) start-of-block when transitioning into
//           LISTEN state so that the first block will be complete.
// TODO TEST Set NETSTAE to idle in IDLE state

//#define _GNU_SOURCE 1
//#include <stdio.h>
//#include <sys/types.h>
#include <stdlib.h>
#include <math.h>
#include <unistd.h>
#include <limits.h>
#include <string.h>
#include <time.h>
#include <pthread.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <poll.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "hashpipe.h"
#include "hpguppi_databuf.h"
#include "hpguppi_pksuwl.h"
#include "hpguppi_time.h"
#include "hpguppi_util.h"

#include "hpguppi_ibverbs_pkt_thread.h"

// Milliseconds between periodic status buffer updates
#define PERIODIC_STATUS_BUFFER_UPDATE_MS (200)

#define ELAPSED_NS(start,stop) \
  (((int64_t)stop.tv_sec-start.tv_sec)*1000*1000*1000+(stop.tv_nsec-start.tv_nsec))

// Define run states.  Currently three run states are defined: IDLE, LISTEN,
// and RECORD.
//
// In the LISTEN and RECORD states, the PKTIDX field is updated with the value
// from received packets.  Whenever the first PKTIDX of a block is received
// (i.e. whenever PKTIDX is a multiple of PKSUWL_PKTIDX_PER_BLOCK), the value
// for PKTSTART and DWELL are read from the status buffer.  PKTSTART is rounded
// down, if needed, to ensure that it is a multiple of PKSUWL_PKTIDX_PER_BLOCK,
// then PKTSTART is written back to the status buffer.  DWELL is interpreted as
// the number of seconds to record and is used to calculate PKTSTOP (which gets
// rounded down, if needed, to be a multiple of PKSUWL_PKTIDX_PER_BLOCK).
//
// The IDLE state is entered when there is no DESTIP defined n the status
// buffer or it is 0.0.0.0.  In the IDLE state, the DESTIP value in the status
// buffer is checked once per second.  If it is found to be something other
// than 0.0.0.0, the state transitions to the LISTEN state and the current
// blocks are reinitialized.
//
// To be operationally compatible with other hpguppi net threads, a "command
// FIFO" is created and read from in all states, but commands sent there are
// ignored.  State transitions are controlled entirely by DESTIP and
// PKTSTART/SWELL status buffer fields.
//
// In the LISTEN state, incoming packets are processed (i.e. stored in the net
// thread's output buffer) and full blocks are passed to the next thread.  When
// the processed PKTIDX is equal to PKTSTART the state transitions to RECORD
// and the following actions occur:
//
//   1. The MJD of the observation start time is calculated from PKTIDX.
//      from SYNCTIME and PKTIDX)
//
//   2. The packet stats counters are reset
//
//   3. The STT_IMDJ and STT_SMJD are updated in the status buffer
//
//   4. STTVALID is set to 1
//
// In the RECORD state, incoming packets are processed (i.e. stored in the net
// thread's output buffer) and full blocks are passed to the next thread (same
// as in the LISTEN state).  When the processed PKTIDX is greater than or equal
// to PKTSTOP the state transitions to LISTEN and STTVALID is set to 0.
//
// The downstream thread (i.e. hpguppi_rawdisk_thread) is expected to use a
// combination of PKTIDX, PKTSTART, PKTSTOP, and (optionally) STTVALID to
// determine whether the blocks should be discarded or processed (e.g. written
// to disk).

enum run_states {IDLE, LISTEN, RECORD};

// Structure related to block management
struct block_info {
  // Set at start of run
  struct hpguppi_input_databuf *db; // Pointer to overall shared mem databuf
  // Set at start of block
  int block_idx;                    // Block index number in databuf
  uint64_t block_num;               // Absolute block number
  // Incremented throughout duration of block
  uint32_t npacket;                 // Number of packets recevied so far
  // Fields set during block finalization
  uint32_t ndrop;                   // Count of expected packets not recevied
};

// Returns pointer to block_info's data
static char * block_info_data(const struct block_info *bi)
{
  return hpguppi_databuf_data(bi->db, bi->block_idx);
}

// Returns pointer to block_info's header
static char * block_info_header(const struct block_info *bi)
{
  return hpguppi_databuf_header(bi->db, bi->block_idx);
}

// Reset counter(s) in block_info
static void reset_block_info_stats(struct block_info *bi)
{
  bi->npacket=0;
  bi->ndrop=0;
}

// (Re-)initialize some or all fields of block_info bi.
// bi->db is set if db is non-NULL.
// bi->block_idx is set if block_idx >= 0.
// bi->block_num is always set and the stats are always reset.
static void init_block_info(struct block_info *bi,
    struct hpguppi_input_databuf *db, int block_idx, uint64_t block_num)
{
  if(db) {
    bi->db = db;
  }
  if(block_idx >= 0) {
    bi->block_idx = block_idx;
  }
  bi->block_num = block_num;
  reset_block_info_stats(bi);
}

// Update block's header info and set filled status (i.e. hand-off to downstream)
static void finalize_block(struct block_info *bi)
{
  if(bi->block_idx < 0) {
    hashpipe_error(__FUNCTION__, "block_info.block_idx == %d", bi->block_idx);
    pthread_exit(NULL);
  }
  char *header = block_info_header(bi);
  char dropstat[128];
  uint64_t pktidx = bi->block_num * PKSUWL_PKTIDX_PER_BLOCK;
  uint64_t pktstart = 0;
  uint64_t pktstop = 0;
  uint32_t sttvalid = 0;

  struct timeval tv;

  int    stt_imjd = 0;
  int    stt_smjd = 0;
  double stt_offs = 0;

  hgetu8(header, "PKTSTART", &pktstart);
  hgetu8(header, "PKTSTOP", &pktstop);
  hgetu4(header, "STTVALID", &sttvalid);

  // Ensure STTVALID in block header is consistent with pktidx/pktstart/pktstop
  // The block's header is a copy of the status buffer when the block was
  // acquired from the ring buffer, but the STTVALID in the status buffer at
  // that time was based on two blocks ago.  This check ensures that the first
  // two blocks of a recording get STTVALID=1.
  if(pktstart <= pktidx && pktidx < pktstop) {
    if(sttvalid != 1) {
      // Calc IMJD/SMJD/OFFS based on PKTSTART
      // This isn't perfect because it assumes recording started at PKTSTART.
      // It's possible that the first block recorded happened after PKTSTART.
      pksuwl_pktidx_to_timeval(pktstart, &tv);
      get_mjd_from_timeval(&tv, &stt_imjd, &stt_smjd, &stt_offs);
      hputu4(header, "STTVALID", 1);
      hputu4(header, "STT_IMJD", stt_imjd);
      hputu4(header, "STT_SMJD", stt_smjd);
      hputr8(header, "STT_OFFS", stt_offs);
    }
  } else {
    if(sttvalid != 0) {
      hputu4(header, "STTVALID", 0);
    }
  }

  // Calculate values for NDROP and DROPSTAT
  bi->ndrop = (2 /*pols*/ * PKSUWL_PKTIDX_PER_BLOCK) - bi->npacket;
  sprintf(dropstat, "%d/%d", bi->ndrop, (2 /*pols*/ * PKSUWL_PKTIDX_PER_BLOCK));

  hputi8(header, "PKTIDX", pktidx);
  hputi4(header, "NPKT", bi->npacket);
  hputi4(header, "NDROP", bi->ndrop);
  hputs(header, "DROPSTAT", dropstat);

  hpguppi_input_databuf_set_filled(bi->db, bi->block_idx);
}

// Advance to next block in data buffer.  This new block will contain
// absolute block block_num.
//
// NB: The caller must wait for the new data block to be free after this
// function returns!
static void increment_block(struct block_info *bi, uint64_t block_num)
{
  if(bi->block_idx < 0) {
    hashpipe_warn(__FUNCTION__,
        "block_info.block_idx == %d", bi->block_idx);
  }
  if(bi->db->header.n_block < 1) {
    hashpipe_error(__FUNCTION__,
        "block_info.db->header.n_block == %d", bi->db->header.n_block);
    pthread_exit(NULL);
  }

  bi->block_idx = (bi->block_idx + 1) % bi->db->header.n_block;
  bi->block_num = block_num;
  reset_block_info_stats(bi);
}

// Wait for a block_info's databuf block to be free, then copy status buffer to
// block's header and clear block's data.  Calling thread will exit on error
// (should "never" happen).  Status buffer updates made after the copy to the
// block's header will not be seen in the block's header (e.g. by downstream
// threads).  Any status buffer fields that need to be updated for correct
// downstream processing of this block must be updated BEFORE calling this
// function.  Note that some of the block's header fields will be set when the
// block is finalized (see finalize_block() for details).
static void wait_for_block_free(const struct block_info * bi,
    hashpipe_status_t * st, const char * status_key)
{
  int rv;
  char netstat[80] = {0};
  char netbuf_status[80];
  int netbuf_full = hpguppi_input_databuf_total_status(bi->db);
  sprintf(netbuf_status, "%d/%d", netbuf_full, bi->db->header.n_block);

  hashpipe_status_lock_safe(st);
  {
    hgets(st->buf, status_key, sizeof(netstat), netstat);
    hputs(st->buf, status_key, "waitfree");
    hputs(st->buf, "NETBUFST", netbuf_status);
  }
  hashpipe_status_unlock_safe(st);

  while ((rv=hpguppi_input_databuf_wait_free(bi->db, bi->block_idx))
      != HASHPIPE_OK) {
    if (rv==HASHPIPE_TIMEOUT) {
      netbuf_full = hpguppi_input_databuf_total_status(bi->db);
      sprintf(netbuf_status, "%d/%d", netbuf_full, bi->db->header.n_block);

      hashpipe_status_lock_safe(st);
      {
        hputs(st->buf, status_key, "blocked");
        hputs(st->buf, "NETBUFST", netbuf_status);
      }
      hashpipe_status_unlock_safe(st);

    } else {
      hashpipe_error("hpguppi_pksuwl_net_thread",
          "error waiting for free databuf");
      pthread_exit(NULL);
    }
  }

  hashpipe_status_lock_safe(st);
  {
    hputs(st->buf, status_key, netstat);
    memcpy(block_info_header(bi), st->buf, HASHPIPE_STATUS_TOTAL_SIZE);
  }
  hashpipe_status_unlock_safe(st);

  bzero_nt(block_info_data(bi), PKSUWL_BLOCK_DATA_SIZE);
}

// The copy_packet_data_to_databuf() function does what it says: copies packet
// data into a data buffer.  The data buffer block is identified by the
// block_info structure pointed to by the bi parameter.  The vdifhdr parameter
// points to the "struct vdifhdr" structure at the beginning of the VDIF
// payload of the packet.  The data array immediately follows the VDIF header
// with a start address of (vdifhdr+1) thanks to pointer arithmetic.
//
// VDIF data in a PKSUWL packet are for a single polarization.  The
// polarization is indicated by a 0 or 1 in the VDIF thread_id field.  The VDIF
// data array in a PKSUWL packet has the following properties:
//
//   1. Only from one of two polarizations with polarization indicated by a 0
//      or 1 in the VDIF header thread_id field.
//   2. Data consist of complex samples.  This fact is represented in the
//      VDIF header, but this code assumes and does does not validate that
//      the header specifies complex.
//   3. Each component of a compelx sample is 16 bits.  This fact is
//      represented in the VDIF header, but this code assumes and does not
//      validate that the header specifies 16 bits.
//   4. Each integer value is represented in offset binary form (0
//      corresponds to the most negative vaslue).  This is mandated by the
//      VDIF spec and is not indicated in the VDIF header.
//   5. The data array contains 8192 bytes (2048 complex samples).  This fact
//      is represented in the VDIF header and this code will ignore packets if
//      the data array length is not 8192 bytes.
//
// Differences between the PKSUWL VDIF format and the GUPPI RAW format require
// manipulation of the packet data as it is copied into the GUPPI RAW formatted
// data buffer.  Specifically, the polarizations must be interleaved and the
// data values must be converted to two's complement form.
//
// This function treats the 16+16 bit complex samples as a single unsigned 32
// bit integer (uint32_t).
static void copy_packet_data_to_databuf(uint64_t packet_idx,
    struct block_info *bi, uint32_t vdif_thread_id, uint8_t * payload)
{
  int i;
  uint32_t * src = (uint32_t *)(payload);
  uint32_t * dst = (uint32_t *)block_info_data(bi);

  // Compute starting packet offset into data block
  off_t offset = (off_t)(packet_idx % PKSUWL_PKTIDX_PER_BLOCK);
  // Convert to sample (i.e. uint32_t) offset
  offset *= 2 /*pols*/ * PKSUWL_SAMPLES_PER_PKT;
  // Adjust for polarization
  offset += (vdif_thread_id & 1);
  // Update destination pointer
  dst += offset;

  // Copy samples
  for(i=0 ; i<PKSUWL_SAMPLES_PER_PKT; i++) {
    // Invert the MSb's of each component to convert to two's complement
    *dst++ = *src++ ^ 0x80008000;
    dst++; // Extra increment to interleave pols
  }
}

// Called periodically to update/query status buffer fields
static
void
update_status_buffer_periodic(hashpipe_status_t *st, int nfull, int nblocks,
    uint64_t nbytes, uint64_t npkts, uint64_t ns_processed,
#if 0
    char *dest_ip_str, size_t dest_ip_len, uint32_t *bind_port,
#endif
    time_t *last_daq_pulse, uint64_t *ndrop)
{
  char timestr[32] = {0};
  char bufst[80];
  double gbps;
  double pps;
  time_t now;
  uint64_t u64tmp;

  // Check DAQPULSE
  time(&now);
  if(*last_daq_pulse != now) {
    ctime_r(&now, timestr);
    timestr[strlen(timestr)-1] = '\0'; // Chop off trailing newline
  }

  // Make xxxBUFST string
  sprintf(bufst, "%d/%d", nfull, nblocks);

  // Calculate stats
  gbps = 8.0 * nbytes / ns_processed;
  pps = 1e9 * npkts / ns_processed;

  // Update status buffer fields
  hashpipe_status_lock_safe(st);
  {
    if(*last_daq_pulse != now) {
      *last_daq_pulse = now;
      hputs(st->buf, "DAQPULSE", timestr);
    }
    hputs(st->buf, "NETBUFST", bufst);
    hputnr8(st->buf, "NETGBPS", 6, gbps);
    hputnr8(st->buf, "NETPPS", 3, pps);

    hgetu8(st->buf, "NDROP", &u64tmp);
    u64tmp += *ndrop; *ndrop = 0;
    hputu8(st->buf, "NDROP", u64tmp);

#if 0
    hgets(st->buf, "DESTIP", dest_ip_len, dest_ip_str);
    hgetu4(st->buf, "BINDPORT", bind_port);
#endif
  }
  hashpipe_status_unlock_safe(st);
}

// Called whenever working block zero changes, either because of normal block
// advance or because of re-init due to packet discontinuity, and returns new
// state (LISTEN or RECORD).
//
// Updates PKTIDX, rounds PKTSTART to proper granularity, uses DWELL and TBIN
// to compute then store PKTSTOP, and then checks the PKTIDX value against the
// status buffer's PKTSTART/PKTSTOP values using logic that goes something like
// this:
//
//   if PKTSTART <= pktidx < PKTSTOPs
//     if STTVALID == 0
//       STTVALID=1
//       calculate and store STT_IMJD, STT_SMJD
//     endif
//     return RECORD
//   else
//     STTVALID=0
//     return LISTEN
//   endif
static
enum run_states
update_status_buffer_new_block(hashpipe_status_t *st, uint64_t pkt_blk)
{
  enum run_states retval = LISTEN;

  uint64_t pktidx = pkt_blk * PKSUWL_PKTIDX_PER_BLOCK;
  uint64_t pktstart = 0;
  uint64_t pktstop = 0;
  uint32_t sttvalid = 0;
  uint64_t dwell_blocks = 0;
  double dwell_seconds = 300.0;
  double tbin = 1/128e6;

  struct timeval tv;

  int    stt_imjd = 0;
  int    stt_smjd = 0;
  double stt_offs = 0;

  hashpipe_status_lock_safe(st);
  {
    hputu8(st->buf, "PKTIDX", pktidx);
    hgetu8(st->buf, "PKTSTART", &pktstart);
    pktstart -= pktstart % PKSUWL_PKTIDX_PER_BLOCK;
    hputu8(st->buf, "PKTSTART", pktstart);
    hgetr8(st->buf, "DWELL", &dwell_seconds);
    hputr8(st->buf, "DWELL", dwell_seconds); // In case it wasn't there
    hgetr8(st->buf, "TBIN", &tbin);
    // Dwell blocks is equal to:
    //
    //       dwell_seconds
    //     ------------------
    //     tbin * ntime/block
    //
    // To get an integer number of blocks, simply truncate
    dwell_blocks = trunc(dwell_seconds
        / (tbin * PKSUWL_SAMPLES_PER_PKT * PKSUWL_PKTIDX_PER_BLOCK));

    pktstop = pktstart + PKSUWL_PKTIDX_PER_BLOCK * dwell_blocks;
    hputi8(st->buf, "PKTSTOP", pktstop);

    hgetu4(st->buf, "STTVALID", &sttvalid);

    // Check start/stop
    if(pktstart <= pktidx && pktidx < pktstop) {
      retval = RECORD;
      hputs(st->buf, "DAQSTATE", "RECORD");

      if(sttvalid != 1) {
        // Calc IMJD/SMJD/OFFS based on PKTSTART
        // This isn't perfect because it assumes recording started at PKTSTART.
        // It's possible that the first block recorded happened after PKTSTART.
        pksuwl_pktidx_to_timeval(pktstart, &tv);
        get_mjd_from_timeval(&tv, &stt_imjd, &stt_smjd, &stt_offs);
        hputu4(st->buf, "STTVALID", 1);
        hputu4(st->buf, "STT_IMJD", stt_imjd);
        hputu4(st->buf, "STT_SMJD", stt_smjd);
        hputr8(st->buf, "STT_OFFS", stt_offs);
      }
    } else {
      hputs(st->buf, "DAQSTATE", "LISTEN");
      if(sttvalid != 0) {
        hputu4(st->buf, "STTVALID", 0);
      }
    }
  }
  hashpipe_status_unlock_safe(st);

  return retval;
}

// This thread's init() function, if provided, is called by the Hashpipe
// framework at startup to allow the thread to perform initialization tasks
// such as setting up network connections or GPU devices.
static
int
init(hashpipe_thread_args_t *args)
{
  // Local aliases to shorten access to args fields
  // Our input buffer happens to be a hpguppi_input_databuf
  hpguppi_input_databuf_t *dbin  = (hpguppi_input_databuf_t *)args->ibuf;
  const char * thread_name = args->thread_desc->name;
  const char * status_key = args->thread_desc->skey;
  hashpipe_status_t *st = &args->st;

  // Non-network essential paramaters
  int blocsize=PKSUWL_BLOCK_DATA_SIZE;
  int directio=1;
  int nbits=16;
  int npol=4;
  // OBSFREQ and OBSBW are different for each instance so we require that they
  // be pre-specified.
  double obsfreq=0;
  double obsbw=0;
  int obsnchan=1;
  int overlap=0;
  double tbin=0.0;
  char obs_mode[80] = {0};
  char dest_ip[80] = {0};

  // Validate chunk sizes in pktbuf_info.  This thread currently
  // expects/requires three chunks with sizes:
  //
  //     0) PKT_OFFSET_PKSUWL_VDIF_HEADER [44] or
  //        PKT_OFFSET_PKSUWL_VDIF_HEADER_8021Q [46]
  //
  //     1) sizeof(stuct vdifhdr) [32]
  //
  //     2) greater than or equal to 8192 [8198]
  //
  // Any other sizing info is a fatal error
  struct hpguppi_pktbuf_info * pbi = hpguppi_pktbuf_info_ptr(dbin);
  if(pbi->num_chunks != 3) {
    hashpipe_error(thread_name, "num_chunks must be 3 (got %u)",
        pbi->num_chunks);
    return HASHPIPE_ERR_PARAM;
  }
  if(pbi->chunks[0].chunk_size != PKT_OFFSET_PKSUWL_VDIF_HEADER &&
     pbi->chunks[0].chunk_size != PKT_OFFSET_PKSUWL_VDIF_HEADER_8021Q) {
    hashpipe_error(thread_name, "chunk 0 size must be %u or %u (got %u)",
        PKT_OFFSET_PKSUWL_VDIF_HEADER,
        PKT_OFFSET_PKSUWL_VDIF_HEADER_8021Q,
        pbi->chunks[0].chunk_size);
    return HASHPIPE_ERR_PARAM;
  }
  if(pbi->chunks[1].chunk_size != sizeof(struct vdifhdr)) {
    hashpipe_error(thread_name, "chunk 1 size must be %u (got %u)",
        sizeof(struct vdifhdr), pbi->chunks[1].chunk_size);
    return HASHPIPE_ERR_PARAM;
  }
  if(pbi->chunks[2].chunk_size < 8192) {
    hashpipe_error(thread_name, "chunk 1 size must be >= 8192 (got %u)",
        pbi->chunks[2].chunk_size);
    return HASHPIPE_ERR_PARAM;
  }

  // Get DESTIP, OBSFREQ, and OBSBW first since their absence is a fatal error
  hashpipe_status_lock_safe(st);
  {
    hgets(st->buf, "DESTIP", sizeof(dest_ip), dest_ip);
    hgetr8(st->buf, "OBSFREQ", &obsfreq);
    hgetr8(st->buf, "OBSBW", &obsbw);
  }
  hashpipe_status_unlock_safe(st);

  if(dest_ip[0] == 0) {
    hashpipe_error(thread_name, "DESTIP not found but is required");
    return HASHPIPE_ERR_PARAM;
  }
  if(obsfreq == 0.0) {
    hashpipe_error(thread_name, "OBSFREQ not found but is required");
    return HASHPIPE_ERR_PARAM;
  }
  if(obsbw == 0.0) {
    hashpipe_error(thread_name, "OBSBW not found but is required");
    return HASHPIPE_ERR_PARAM;
  }

  // Set default values for strings
  strcpy(obs_mode, "RAW");

  hashpipe_status_lock_safe(st);
  {
    // Get info from status buffer if present (no change if not present)
    hgeti4(st->buf, "BLOCSIZE", &blocsize);
    hgeti4(st->buf, "DIRECTIO", &directio);
    //hgeti4(st->buf, "NBITS", &nbits); // Force to 16 for UWL
    hgeti4(st->buf, "NPOL", &npol);
    //hgeti4(st->buf, "OBSNCHAN", &obsnchan); // Force to 1 for UWL
    hgeti4(st->buf, "OVERLAP", &overlap);
    hgets(st->buf, "OBS_MODE", sizeof(obs_mode), obs_mode);

    // Calculate TBIN from OBSNCHAN and OBSBW
    tbin = fabs(obsnchan / obsbw) / 1e6;

    // Store info in status buffer (in case it was not there before).
    hputi4(st->buf, "BLOCSIZE", blocsize);
    hputi4(st->buf, "DIRECTIO", directio);
    hputi4(st->buf, "NBITS", nbits);
    hputi4(st->buf, "NPOL", npol);
    hputr8(st->buf, "CHAN_BW", obsbw);     // CHAN_BW == OBSBW
    hputi4(st->buf, "OBSNCHAN", obsnchan); // Force to 1 for UWL
    hputi4(st->buf, "OVERLAP", overlap);
    // Force PKTFMT to be "VDIF"
    hputs(st->buf, "PKTFMT", "VDIF");
    hputr8(st->buf, "TBIN", tbin);
    hputs(st->buf, "OBS_MODE", obs_mode);
    hputi4(st->buf, "NDROP", 0);
    // Set status_key to init
    hputs(st->buf, status_key, "init");
  }
  hashpipe_status_unlock_safe(st);

  // Success!
  return 0;
}

static
void *
run(hashpipe_thread_args_t * args)
{
  // Local aliases to shorten access to args fields
  // Our input and output buffers happen to be a hpguppi_input_databuf
  hpguppi_input_databuf_t *dbin  = (hpguppi_input_databuf_t *)args->ibuf;
  hpguppi_input_databuf_t *dbout = (hpguppi_input_databuf_t *)args->obuf;
  hashpipe_status_t *st = &args->st;
  const char * thread_name = args->thread_desc->name;
  const char * status_key = args->thread_desc->skey;

  // Current run state
  enum run_states state = IDLE;
  unsigned waiting = 0;
  // Update status_key with idle state
  hashpipe_status_lock_safe(st);
  {
    hputs(st->buf, status_key, "idle");
  }
  hashpipe_status_unlock_safe(st);

  // Misc counters, etc
  int i;
  // String version of destination address
  char dest_ip_str_new[80] = {0};
  char dest_ip_str_cur[80] = {0};
  // Numeric form of dest_ip
  struct in_addr dest_ip;
  // Destination UDP port
  uint32_t bind_port = 0;

  // The incoming packets are placed in blocks that are eventually passed off
  // to the downstream thread.  We currently support two active blocks (aka
  // "working blocks").  Working blocks are associated with absolute block
  // numbers, which are simply PKTIDX values divided by the number of packets
  // per block (discarding any remainder).  Let the block numbers for the first
  // working block (wblk[0]) be W.  The block number for the second working
  // block (wblk[1]) will be W+1.  Incoming packets corresponding to block W or
  // W+1 are placed in the corresponding data buffer block.  Incoming packets
  // for block W+2 cause block W to be "finalized" and handed off to the
  // downstream thread, working block 1 moves to working block 0 and working
  // block 1 is incremented to be W+2.  Things get "interesting" when a packet
  // is recevied for block < W or block > W+2.  Packets for block W-1 are
  // ignored.  Packets with PKTIDX P corresponding block < W-1 or block > W+2
  // cause the current working blocks' block numbers to be reset such that W
  // will refer to the block containing P and W+1 will refer to the block after
  // that.
  //
  // wblk is a two element array of block_info structures (i.e. the working
  // blocks)
  struct block_info wblk[2];
  int wblk_idx;

  // Packet block variables
  uint64_t pkt_seq_num;
  uint64_t pkt_blk_num;

  // Heartbeat variables
  time_t last_daqpulse = 0;

  // Variables for working with the input databuf
  struct hpguppi_pktbuf_info * pktbuf_info = hpguppi_pktbuf_info_ptr(dbin);
  int block_idx_in = 0;
  int timed_out = 0;
  const int npkts_per_block_in = pktbuf_info->slots_per_block;
  const size_t slot_size = pktbuf_info->slot_size;
  struct timespec timeout_in = {0, 50 * 1000 * 1000}; // 50 ms

  // Variables for counting packets and bytes.
  uint64_t ndrop_total = 0;
  uint64_t nlate = 0;
  // TODO Move used variabled from above to below, then remove unused
  uint64_t bytes_received = 0;
  uint64_t pkts_received = 0;

  // Variables for handing received packets
  struct vdifhdr * vdifhdr;
  uint8_t * payload;
  off_t vdifhdr_offset = pktbuf_info->chunks[1].chunk_offset;
  off_t payload_offset = pktbuf_info->chunks[2].chunk_offset;
  const size_t bytes_per_packet = pktbuf_info->pkt_size;

  // Variables for tracking timing stats
  struct timespec ts_start_recv, ts_stop_recv;
  uint64_t ns_processed = 0;
  struct timespec ts_last_update = {0};
  uint64_t ns_since_last_update = 0;

  // Initialize working blocks
  for(wblk_idx=0; wblk_idx<2; wblk_idx++) {
    init_block_info(wblk+wblk_idx, dbout, wblk_idx, wblk_idx);
    wait_for_block_free(wblk+wblk_idx, st, status_key);
  }

  // Wait for ibvpkt thread to be running, then it's OK to add/remove flows.
  hpguppi_ibvpkt_wait_running(st);

  // Get DESTIP ands BINDPORT
  hashpipe_status_lock_safe(st);
  {
    hgets(st->buf, "DESTIP", sizeof(dest_ip_str_new), dest_ip_str_new);
    hgetu4(st->buf, "BINDPORT", &bind_port);
  }
  hashpipe_status_unlock_safe(st);

  // Initial ts_stop_recv
  clock_gettime(CLOCK_MONOTONIC_RAW, &ts_stop_recv);

  // Main loop
  while (run_threads()) {

    // Wait for data
    do {
      // Capture start time of the wait-for-filled-block
      clock_gettime(CLOCK_MONOTONIC_RAW, &ts_start_recv);
      // stop_recv to start_recv is processing time
      ns_processed += ELAPSED_NS(ts_stop_recv, ts_start_recv);

      //
      // Wait for the filled block
      //
      timed_out = hpguppi_input_databuf_wait_filled_timeout(
          dbin, block_idx_in, &timeout_in);

      // Capture stop time of the wait-for-filled-block
      clock_gettime(CLOCK_MONOTONIC_RAW, &ts_stop_recv);

      //
      // Check for periodic status buffer update interval
      //
      ns_since_last_update = ELAPSED_NS(ts_last_update, ts_stop_recv);
      if(ns_since_last_update >= PERIODIC_STATUS_BUFFER_UPDATE_MS*1000*1000) {
        // Save new last update time
        ts_last_update = ts_stop_recv;

        // Update status buffer
        update_status_buffer_periodic(st,
            hpguppi_input_databuf_total_status(dbout), dbout->header.n_block,
            bytes_received, pkts_received, ns_processed,
#if 0
            dest_ip_str_new, sizeof(dest_ip_str_new), &bind_port,
#endif
            &last_daqpulse, &ndrop_total);

        // If DESTIP changed
        if(strcmp(dest_ip_str_cur, dest_ip_str_new)) {
          // Parse new DESTIP
          if(!inet_aton(dest_ip_str_new, &dest_ip)) {
            hashpipe_warn(thread_name,
                "invalid DESTIP adress %s being treated as 0.0.0.0",
                dest_ip_str_new);
            dest_ip.s_addr = 0;
          }

          // If dest_ip is 0 (i.e. DESTIP is 0.0.0.0)
          if(dest_ip.s_addr == 0) {
#if 0
            // Save "0.0.0.0" as current DESTIP
            strcpy(dest_ip_str_cur, dest_ip_str_new);

            hashpipe_info(thread_name,
                "DESTIP %s: removing flow", dest_ip_str_new);

            // Remove flow
            if(hpguppi_ibvpkt_flow(dbin, 0, IBV_FLOW_SPEC_UDP,
                  0, 0, 0, 0, 0, 0, 0, 0))
            {
              hashpipe_error(thread_name, "hashpipe_ibv_flow error");
              errno = 0;
            }
            // Switch to IDLE state (and ensure waiting flag is clear)
            state = IDLE;
            waiting = 0;
#endif
          } else {
            // dest_ip!=0, only recognize if state is IDLE
            if(state == IDLE) {
              // Save new DESTIP as current DESTIP
              strcpy(dest_ip_str_cur, dest_ip_str_new);

              hashpipe_info(thread_name,
                  "DESTIP %s: adding flow", dest_ip_str_new);

              // Add flow
              if(hpguppi_ibvpkt_flow(dbin, 0, IBV_FLOW_SPEC_UDP,
                    NULL, NULL, 0, 0, 0, ntohl(dest_ip.s_addr), 0, bind_port))
              {
                hashpipe_error(thread_name, "hashpipe_ibv_flow error");
                errno = 0;
              }
              // Switch to LISTEN state (and ensure waiting flag is clear)
              state = LISTEN;
              waiting = 0;
            } else {
              hashpipe_warn(thread_name,
                  "DESTIP %s: ignored in non-IDLE state", dest_ip_str_new);
            }
          } // dest_ip == 0

          // Store DESTIP (e.g. to overwrite invalid external request) and
          // update DAQSTATE.
          hashpipe_status_lock_safe(st);
          {
            hputs(st->buf, "DESTIP", dest_ip_str_new);
            hputs(st->buf, "DAQSTATE", state == IDLE   ? "IDLE"   :
                                       state == LISTEN ? "LISTEN" : "RECORD");
          }
          hashpipe_status_unlock_safe(st);
        } // DESTIP changed
      } // End 50 ms update

      // Set status field to "waiting" if we are not getting packets, threads
      // are still running, state is not IDLE, and waiting it not already set
      if (timed_out && run_threads() && state != IDLE && !waiting) {
        hashpipe_status_lock_safe(st);
        {
          hputs(st->buf, status_key, "waiting");
        }
        hashpipe_status_unlock_safe(st);
        waiting=1;
      }
    } while(timed_out && run_threads()); // end wait for data loop

    if(!run_threads()) {
      // We're outta here!
      // But first mark the block free if we got one.
      if(!timed_out) {
        hpguppi_input_databuf_set_free(dbin, block_idx_in);
        // No need to advance block_idx_in since we are outta here
      }
      break;
    } else if(state == IDLE) {
      // Go back to top of main loop
      // But first mark the block free if we got one
      // If we got here, we must have got one (right?), but we check anyway
      if(!timed_out) {
        hpguppi_input_databuf_set_free(dbin, block_idx_in);
        // Advance to next input block
        block_idx_in = (block_idx_in + 1) % dbin->header.n_block;
      }
      continue;
    }

    // Got packet(s)!  Update status if needed.
    if (waiting) {
      hashpipe_status_lock_safe(st);
      {
        hputs(st->buf, status_key, "receiving");
      }
      hashpipe_status_unlock_safe(st);
      waiting=0;
    }

    // Get pointer to first vdifhdr and payload
    vdifhdr = (struct vdifhdr *)
              (dbin->block[block_idx_in].data + vdifhdr_offset);
    payload = (uint8_t *)
              (dbin->block[block_idx_in].data + payload_offset);//pbi->chunks[2].chunk_offset);

    // For each packet: process all packets
    for(i=0; i < npkts_per_block_in; i++, payload += slot_size,
        vdifhdr = (struct vdifhdr *)(((uint8_t *)vdifhdr) + slot_size)) {

      // TODO Validate that this is a valid packet for us!

      // Count packet and bytes, even if we ultimately ignore packet
      pkts_received++;
      bytes_received += bytes_per_packet;

      // Warn about unexpected data array length and ignore
      if(vdif_get_data_array_length(vdifhdr) != 8192) {
        hashpipe_warn(thread_name, "unexpected data array length %u",
            vdif_get_data_array_length(vdifhdr));
        continue;
      }

      // Get packet index and absolute block number for packet
      pkt_seq_num = pksuwl_get_pktidx(vdifhdr);
      pkt_blk_num = pkt_seq_num / PKSUWL_PKTIDX_PER_BLOCK;

      // Manage blocks based on pkt_blk_num
      if(pkt_blk_num == wblk[1].block_num + 1) {
        // Finalize first working block
        finalize_block(wblk);
        // Update ndrop counter
        ndrop_total += wblk->ndrop;
        if(wblk->ndrop >= PKSUWL_PKTIDX_PER_BLOCK) {
          // Assume one entire polrization is missing
          ndrop_total -= PKSUWL_PKTIDX_PER_BLOCK;
        }
        // Shift working blocks
        wblk[0] = wblk[1];

        // Increment last working block
        increment_block(&wblk[1], pkt_blk_num);
        // Wait for new databuf data block to be free
        wait_for_block_free(&wblk[1], st, status_key);

        // Update status buffer for new wblk[0]
        state = update_status_buffer_new_block(st, wblk[0].block_num);
      }
      // Check for PKTIDX discontinuity
      else if(pkt_blk_num < wblk[0].block_num - 1
           || pkt_blk_num > wblk[1].block_num + 1) {

        // Should only happen when transitioning into LISTEN, so warn about it
        hashpipe_warn(thread_name,
            "working blocks reinit due to packet discontinuity (PKTIDX %lu)",
            pkt_seq_num);

        // Re-init working blocks for next block number
        // and clear their data buffers
        for(wblk_idx=0; wblk_idx<2; wblk_idx++) {
          init_block_info(wblk+wblk_idx, NULL, -1, pkt_blk_num+wblk_idx+1);
          // Clear data buffer
          bzero_nt(block_info_data(wblk+wblk_idx), PKSUWL_BLOCK_DATA_SIZE);
        }

        // Update status buffer for newly reset wblk[0]
        state = update_status_buffer_new_block(st, wblk[0].block_num);

        // Continue on to next packet
        continue;

      } else if(pkt_blk_num == wblk[0].block_num - 1) {
        // Ignore late packet, continue on to next one.  This happens after
        // discontinuities (e.g. on startup), so don't warn about it.
        nlate++;
        continue;
      }

      // Once we get here, compute the index of the working block corresponding
      // to this packet.  The computed index should correspond to a valid
      // working block, but we validate just to be safe.
      wblk_idx = pkt_blk_num - wblk[0].block_num;

      // Only copy packet data and count packet if its wblk_idx is valid
      if(0 <= wblk_idx && wblk_idx < 2) {
        // Copy packet data to data buffer of working block
        copy_packet_data_to_databuf(pkt_seq_num, wblk+wblk_idx,
            vdif_get_thread_id(vdifhdr), payload);

        // Count packet for block
        wblk[wblk_idx].npacket++;
      }
    } // end for each packet

    // Mark input block free
    hpguppi_input_databuf_set_free(dbin, block_idx_in);

    // Advance to next input block
    block_idx_in = (block_idx_in + 1) % dbin->header.n_block;

    // Will exit if thread has been cancelled
    pthread_testcancel();
  } // end main loop

  hashpipe_info(thread_name, "exiting!");

  return NULL;
}

static hashpipe_thread_desc_t thread_desc = {
    name: "hpguppi_pksuwl_vdif_thread",
    skey: "NETSTAT",
    init: init,
    run:  run,
    ibuf_desc: {hpguppi_input_databuf_create},
    obuf_desc: {hpguppi_input_databuf_create}
};

static __attribute__((constructor)) void ctor()
{
  register_hashpipe_thread(&thread_desc);
}

// vi: set ts=2 sw=2 et :
