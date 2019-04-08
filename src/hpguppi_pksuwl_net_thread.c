// hpguppi_pksuwl_net_thread.c
//
// A Hashpipe thread that uses ibverbs to receive Parkes UWL packets from a
// network interface.

//#define _GNU_SOURCE 1
//#include <stdio.h>
//#include <sys/types.h>
//#include <stdlib.h>
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
#include "hashpipe_ibverbs.h"

#include "hpguppi_databuf.h"
#include "hpguppi_pksuwl.h"

#define HPGUPPI_DAQ_CONTROL "/tmp/hpguppi_daq_control"
#define MAX_CMD_LEN 1024

#define DEFAULT_MAX_PKT_SIZE (8400)
#define DEFAULT_SEND_PKT_NUM (1)
#define DEFAULT_RECV_PKT_NUM (16351)
#define DEFAULT_MAX_FLOWS (1)

// This code supports two possible PKSUWL_BLOCK_DATA_SIZE values.  One for a
// power of two number of samples per block, and one for 1e6 times a power of
// two samples per block.  The different block sizes lead to a different number
// of packets per block per polarization, referred to as PKTIDX_PER_BLOCK.
// Because PKTIDX values are shared across multiple polarizations, this is
// inherently the number of PKTIDX per block per polarization.  The number of
// packets per block is 2*PKTS_PER_PKTIDX*PKTIDX_PER_BLOCK, but PKTS_PER_PKTIDX
// is 1 so that value is taken to be imlicit and is not explicitly defined.

#ifdef USE_POWER_OF_TWO_NCHAN
// If we use a power of two number of channels, the GUPPI RAW block is the same
// as the shared memory block size and the number of PKTIDX values per block is
// 8192.
#define PKSUWL_BLOCK_DATA_SIZE (BLOCK_DATA_SIZE)
#define PKTIDX_PER_BLOCK (8192)
#else
// For the 2**N * 1e6 channel options, we find that 5**5 * 2 == 6250 packets
// per polarization span 0.1 seconds and both polarizations would occupy a
// total of 102,400,000 bytes (clearly less than 128 MiB):
//
//     2**10 * 1e5 bytes == 2**13 bytes/pkt * (5**5 * 2) pkts/pol * 2 pols
#define PKSUWL_BLOCK_DATA_SIZE (1024*10*1000) // in bytes
#define PKTIDX_PER_BLOCK (6250)
#endif // USE_POWER_OF_TWO_NCHAN

#define ELAPSED_NS(start,stop) \
  (((int64_t)stop.tv_sec-start.tv_sec)*1000*1000*1000+(stop.tv_nsec-start.tv_nsec))

// Define run states.  Currently three run states are defined: IDLE, LISTEN,
// and RECORD.
//
// In the LISTEN and RECORD states, the PKTIDX field is updated with the value
// from received packets.  Whenever the first PKTIDX of a block is received
// (i.e. whenever PKTIDX is a multiple of PKTIDX_PER_BLOCK), the value for
// PKTSTART and DWELL are read from the status buffer.  PKTSTART is rounded
// down, if needed, to ensure that it is a multiple of PKTIDX_PER_BLOCK, then
// PKTSTART is written back to the status buffer.  DWELL is interpreted as the
// number of seconds to record and is used to calculate PKTSTOP (which gets
// rounded down, if needed, to be a multiple of PKTIDX_PER_BLOCK).
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
  bi->ndrop = (2 /*pols*/ * PKTIDX_PER_BLOCK) - bi->npacket;
  sprintf(dropstat, "%d/%d", bi->ndrop, (2 /*pols*/ * PKTIDX_PER_BLOCK));
  hputi8(header, "PKTIDX", bi->block_num * PKTIDX_PER_BLOCK);
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
  char netbuf_status[81];
  int netbuf_full = hpguppi_input_databuf_total_status(bi->db);
  sprintf(netbuf_status, "%d/%d", netbuf_full, bi->db->header.n_block);
  hashpipe_status_lock_safe(st);
  hputs(st->buf, status_key, "waitfree");
  hputs(st->buf, "NETBUFST", netbuf_status);
  hashpipe_status_unlock_safe(st);
  while ((rv=hpguppi_input_databuf_wait_free(bi->db, bi->block_idx))
      != HASHPIPE_OK) {
    if (rv==HASHPIPE_TIMEOUT) {
      netbuf_full = hpguppi_input_databuf_total_status(bi->db);
      sprintf(netbuf_status, "%d/%d", netbuf_full, bi->db->header.n_block);
      hashpipe_status_lock_safe(st);
      hputs(st->buf, status_key, "blocked");
      hputs(st->buf, "NETBUFST", netbuf_status);
      hashpipe_status_unlock_safe(st);
    } else {
      hashpipe_error("hpguppi_pksuwl_net_thread",
          "error waiting for free databuf");
      pthread_exit(NULL);
    }
  }
  hashpipe_status_lock_safe(st);
  hputs(st->buf, status_key, "receiving");
  memcpy(block_info_header(bi), st->buf, HASHPIPE_STATUS_TOTAL_SIZE);
  hashpipe_status_unlock_safe(st);

  memset(block_info_data(bi), 0, PKSUWL_BLOCK_DATA_SIZE);
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
//      is represented in the VDIF header, but this code assumes and does not
//      verify the the header specifies 8192 bytes.
//
// Differences between the PKSUWL VDIF format and the GUPPI RAW format require
// manipulation of the packet data as it is copied into the GUPPI RAW formatted
// data buffer.  Specifically, the polarizations must be interleaved and the
// data values must be converted to two's complex form.
//
// This function treats the 16+16 bit complex samples as a single unsigned 32
// bit integer (uint32_t).
static void copy_packet_data_to_databuf(uint64_t packet_idx,
    struct block_info *bi, struct vdifhdr * vdifhdr)
{
  int i;
  uint32_t * src = (uint32_t *)(vdifhdr + 1);
  uint32_t * dst = (uint32_t *)block_info_data(bi);

  // Compute starting packet offset into data block
  off_t offset = (off_t)(packet_idx % PKTIDX_PER_BLOCK);
  // Convert to sample (i.e. uint32_t) offset
  offset *= 2 /*pols*/ * PKSUWL_SAMPLES_PER_PKT;
  // Adjust for polarization
  offset += (vdif_get_thread_id(vdifhdr) & 1);
  // Update destination pointer
  dst += offset;

  // Copy samples
  for(i=0 ; i<PKSUWL_SAMPLES_PER_PKT; i++) {
    // Invert the MSb's of each component to convert to two's complement
    *dst++ = *src++ ^ 0x80008000;
    dst++; // Extra increment to interleave pols
  }
}

// Hashpipe threads typically perform some setup tasks in their init()
// function.  Usually this results in some sort of resource (e.g. open sockets)
// or other state information that needs to be passed to the run() function.
// The mechanism that Hashpipe provides to support this is the `user_data`
// field of the hashpipe_thread_args parameter that is passed to both init()
// and run().  The init() function can allocate memory to hold this shared
// state information and then store a poitner to that memory in the "user_data"
// field of the hashpipe_thread_args paramter before returning.  The run()
// funciton can then access the shared state information through the
// "user_data" pointer of its hashpipe_thread_args paramter.
//
// Here we define a "net_params" structure to hold parameters related to the network
// connection that will be  setup in init() and used in run().
//
// - ifname is the local network interface from which we will receive packets.
// - port is the UDP port which we will listen to.
// - hibv_ctx is the hashpipe_ibv_context structure that Hashpipe uses to
//   manage the packet socket connection.
struct net_params {
  char ifname[IFNAMSIZ];
  int port;
  struct hashpipe_ibv_context hibv_ctx;
};

// This thread's init() function, if provided, is called by the Hashpipe
// framework at startup to allow the thread to perform initialization tasks
// such as setting up network connections or GPU devices.
static int init(hashpipe_thread_args_t *args)
{
  // Non-network essential paramaters
  int blocsize=PKSUWL_BLOCK_DATA_SIZE;
  int directio=1;
  int nbits=16;
  int npol=4;
  double obsfreq=0;
  double obsbw=128.0;
  int obsnchan=1;
  int overlap=0;
  double tbin=0.0;
  char obs_mode[80];
  char dest_ip[80];
  char fifo_name[PATH_MAX];
  struct net_params * net_params;

  // Create control FIFO (/tmp/hpguppi_daq_control/$inst_id)
  int rv = mkdir(HPGUPPI_DAQ_CONTROL, 0777);
  if (rv!=0 && errno!=EEXIST) {
    hashpipe_error("hpguppi_pksuwl_net_thread", "Error creating control fifo directory");
    return HASHPIPE_ERR_SYS;
  } else if(errno == EEXIST) {
    errno = 0;
  }

  sprintf(fifo_name, "%s/%d", HPGUPPI_DAQ_CONTROL, args->instance_id);
  rv = mkfifo(fifo_name, 0666);
  if (rv!=0 && errno!=EEXIST) {
    hashpipe_error("hpguppi_pksuwl_net_thread", "Error creating control fifo");
    return HASHPIPE_ERR_SYS;
  } else if(errno == EEXIST) {
    errno = 0;
  }

  strcpy(obs_mode, "RAW");
  strcpy(dest_ip, "0.0.0.0");

  // Allocate (and clear) net_params structure
  net_params = (struct net_params *)calloc(1, sizeof(struct net_params));
  if(!net_params) {
    return HASHPIPE_ERR_SYS;
  }

  // Set defaults
  strcpy(net_params->ifname, "eth4");
  net_params->port = 12345;

  hashpipe_status_t st = args->st;

  hashpipe_status_lock_safe(&st);
  {
    // Get info from status buffer if present (no change if not present)
    hgets(st.buf,  "BINDHOST", sizeof(net_params->ifname), net_params->ifname);
    hgeti4(st.buf, "BINDPORT", &net_params->port);
    hgeti4(st.buf, "BLOCSIZE", &blocsize);
    hgeti4(st.buf, "DIRECTIO", &directio);
    hgeti4(st.buf, "NBITS", &nbits);
    hgeti4(st.buf, "NPOL", &npol);
    hgetr8(st.buf, "OBSFERQ", &obsfreq);
    hgetr8(st.buf, "OBSBW", &obsbw);
    //hgeti4(st.buf, "OBSNCHAN", &obsnchan); // Force to 1 for UWL
    hgeti4(st.buf, "OVERLAP", &overlap);
    hgets(st.buf, "OBS_MODE", 80, obs_mode);
    hgets(st.buf, "DESTIP", 80, dest_ip);

    // Calculate TBIN from OBSNCHAN and OBSBW
    tbin = fabs(obsnchan / obsbw) / 1e6;

    // Store bind host/port info etc in status buffer (in case it was not there
    // before).
    hputs(st.buf, "BINDHOST", net_params->ifname);
    hputi4(st.buf, "BINDPORT", net_params->port);
    hputi4(st.buf, "BLOCSIZE", blocsize);
    hputi4(st.buf, "DIRECTIO", directio);
    hputi4(st.buf, "NBITS", nbits);
    hputi4(st.buf, "NPOL", npol);
    hputr8(st.buf, "OBSBW", obsbw);
    hputi4(st.buf, "OBSNCHAN", obsnchan); // Force to 1 for UWL
    hputi4(st.buf, "OVERLAP", overlap);
    // Force PKTFMT to be "VDIF"
    hputs(st.buf, "PKTFMT", "VDIF");
    hputr8(st.buf, "TBIN", tbin);
    hputs(st.buf, "OBS_MODE", obs_mode);
    hputs(st.buf, "DESTIP", dest_ip);
    // Init stats fields to 0
    hputu8(st.buf, "NPKTS", 0);
  }
  hashpipe_status_unlock_safe(&st);

  // Store net_params pointer in args
  args->user_data = net_params;

  // Success!
  return 0;
}

static void * run(hashpipe_thread_args_t * args)
{
  // Local aliases to shorten access to args fields
  // Our output buffer happens to be a hpguppi_input_databuf
  hpguppi_input_databuf_t *db = (hpguppi_input_databuf_t *)args->obuf;
  hashpipe_status_t st = args->st;
  const char * status_key = args->thread_desc->skey;

  // Get a pointer to the net_params structure allocated and initialized in
  // init() as well as its hashpipe_ibv_context structure.
  struct net_params *net_params = (struct net_params *)args->user_data;
  struct hashpipe_ibv_context * hibv_ctx = &net_params->hibv_ctx;

  // Open command FIFO for read
  struct pollfd pollfd;
  char fifo_name[PATH_MAX];
  char fifo_cmd[MAX_CMD_LEN];
  char * pchar;
  sprintf(fifo_name, "%s/%d", HPGUPPI_DAQ_CONTROL, args->instance_id);
  int fifo_fd = open(fifo_name, O_RDONLY | O_NONBLOCK);
  if (fifo_fd<0) {
      hashpipe_error("hpguppi_pksuwl_net_thread", "Error opening control fifo)");
      pthread_exit(NULL);
  }
  pollfd.fd = fifo_fd;
  pollfd.events = POLLIN;

  // Current run state
  enum run_states state = IDLE;

  // Misc counters, etc
  int rv;
  // String version of destination address
  char dest_ip_str[80] = {};
  // Numeric form of dest_ip
  struct in_addr dest_ip;
  //TODO?unsigned force_new_block=0;
  unsigned waiting=-1;

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
  uint64_t start_seq_num=0;
  uint64_t stop_seq_num=0;
  //uint64_t last_seq_num=2048;
  //uint64_t nextblock_seq_num=0;
  uint64_t dwell_blocks = 0;
  double dwell_seconds = 300.0;
  double tbin = 1.0/PKSUWL_SAMPLES_PER_SEC;

  // Heartbeat variables
  time_t lasttime = 0;
  time_t curtime = 0;
  char timestr[32] = {0};

  // Variables for counting packets and bytes.
  uint64_t packet_count = 0; // Counts packets between updates to status buffer
  uint64_t u64tmp = 0; // Used for status buffer interactions
  uint64_t max_recvpkt_count = 0;
  uint64_t ndrop_total = 0;

  // Variables for handing received packets
  struct hashpipe_ibv_recv_pkt * hibv_rpkt = NULL;
  struct hashpipe_ibv_recv_pkt * curr_rpkt;
  struct vdifhdr * vdifhdr;

  // Variables for tracking timing stats
  struct timespec ts_start_recv, ts_stop_recv;
  uint64_t elapsed_recv = 0;
  uint64_t count_recv = 0;
  struct timespec ts_start_stat, ts_stop_stat;
  uint64_t elapsed_stat = 0;
  uint64_t count_stat = 0;
  struct timespec ts_start_proc, ts_stop_proc;
  //TODO?uint64_t elapsed_proc = 0;
  //TODO?uint64_t count_proc = 0;

  // Set up ibverbs context
  strncpy(hibv_ctx->interface_name, net_params->ifname, IFNAMSIZ);
  hibv_ctx->interface_name[IFNAMSIZ-1] = '\0'; // Ensure NUL termination
  hibv_ctx->send_pkt_num = DEFAULT_SEND_PKT_NUM;
  hibv_ctx->recv_pkt_num = DEFAULT_RECV_PKT_NUM;
  hibv_ctx->pkt_size_max = DEFAULT_MAX_PKT_SIZE;
  hibv_ctx->max_flows    = DEFAULT_MAX_FLOWS;

  // Get params from status buffer (if present)
  hashpipe_status_lock_safe(&st);
  {
    // Read (no change if not present)
    hgetu4(st.buf, "RPKTNUM", &hibv_ctx->recv_pkt_num);
    hgetu4(st.buf, "MAXFLOWS", &hibv_ctx->max_flows);
  }
  hashpipe_status_unlock_safe(&st);

  hashpipe_info(args->thread_desc->name, "recv_pkt_num=%u max_flows=%u",
      hibv_ctx->recv_pkt_num, hibv_ctx->max_flows);

  // Initialize ibverbs
  if(hashpipe_ibv_init(hibv_ctx)) {
    hashpipe_error("hpguppi_pksuwl_net_thread", "Error initializing ibverbs.");
    return NULL;
  }

  // Initialize working blocks
  for(wblk_idx=0; wblk_idx<2; wblk_idx++) {
    init_block_info(wblk+wblk_idx, db, wblk_idx, wblk_idx);
    wait_for_block_free(wblk+wblk_idx, &st, status_key);
  }

  // Main loop
  while (run_threads()) {

    while(state == IDLE) {
      // Poll command line fifo with 100 ms timeout
      rv = poll(&pollfd, 1, 100);
      if(rv > 0) {
        // Read any/all commands and ignore them
        while((rv = read(fifo_fd, fifo_cmd, MAX_CMD_LEN-1)) > 0) {
          // Truncate at first newline
          fifo_cmd[MAX_CMD_LEN-1] = '\0';
          if((pchar = strchr(fifo_cmd, '\n'))) {
            *pchar = '\0';
          }
          hashpipe_warn("hpguppi_pksuwl_net_thread",
              "ignoring %s command", fifo_cmd);
        }
      } else if(rv < 0) {
        hashpipe_error("hpguppi_pksuwl_net_thread", "command fifo poll error");
        // Bail out (this should "never" happen)
        break;
      }

      // We perform some status buffer updates every second
      time(&curtime);
      if(curtime != lasttime) {
        ctime_r(&curtime, timestr);
        timestr[strlen(timestr)-1] = '\0'; // Chop off trailing newline
      }

      // Check dest_ip/port in status buffer and, if needed, update DAQPULSE
      hashpipe_status_lock_safe(&st);
      {
        // Get DESTIP address and BINDPORT (a historical misnomer for DESTPORT)
        hgets(st.buf,  "DESTIP", sizeof(dest_ip_str), dest_ip_str);
        hgeti4(st.buf,  "BINDPORT", &net_params->port);
        if(curtime != lasttime) {
          lasttime = curtime;
          hputs(st.buf,  "DAQPULSE", timestr);
        }
      }
      hashpipe_status_unlock_safe(&st);

      // If DESTIP is valid and non-zero, start listening!  Valid here just
      // means that it parses OK via inet_aton, not that it is correct and
      // actually usable.
      if(inet_aton(dest_ip_str, &dest_ip) && dest_ip.s_addr != INADDR_ANY) {
        // Add flow and change state to listen
        if(hashpipe_ibv_flow(hibv_ctx, 0, IBV_FLOW_SPEC_UDP,
              hibv_ctx->mac, NULL, 0, 0,
              0, ntohl(dest_ip.s_addr), 0, net_params->port))
        {
          hashpipe_error(
              "hpguppi_pksuwl_net_thread", "hashpipe_ibv_flow error");
          // Stay in idle state loop
          continue;
        }

        state = LISTEN;
        // Update DAQSTATE
        hashpipe_status_lock_safe(&st);
        {
          // Update DAQSTATE
          hputs(st.buf,  "DAQSTATE", "LISTEN");
        }
        hashpipe_status_unlock_safe(&st);
      }
    } // end while state == IDLE

    // Wait for data
    do {
      clock_gettime(CLOCK_MONOTONIC_RAW, &ts_start_recv);
      hibv_rpkt = hashpipe_ibv_recv_pkts(hibv_ctx, 1000); // 1 second timeout
      clock_gettime(CLOCK_MONOTONIC_RAW, &ts_stop_recv);

      if(!hibv_rpkt) {
        if(errno) {
          perror("hashpipe_ibv_recv_pkt");
          continue;
        }
        // Timeout?
        time(&curtime);
        if(curtime == lasttime) {
          // No, continue receiving
          continue;
        }
      }

      // Got packets or timeout

      // We perform some status buffer updates every second
      clock_gettime(CLOCK_MONOTONIC_RAW, &ts_start_stat);
      time(&curtime);
      if(curtime != lasttime) {
        lasttime = curtime;
        ctime_r(&curtime, timestr);
        timestr[strlen(timestr)-1] = '\0'; // Chop off trailing newline

        hashpipe_status_lock_safe(&st);
        {
          hputs(st.buf, "DAQPULSE", timestr);

          hgetu8(st.buf, "NPKTS", &u64tmp);
          u64tmp += packet_count; packet_count = 0;
          hputu8(st.buf, "NPKTS", u64tmp);

          // Get DESTIP to see if we should go to IDLE state
          hgets(st.buf,  "DESTIP", sizeof(dest_ip_str), dest_ip_str);
        }
        hashpipe_status_unlock_safe(&st);

        // If DESTIP is invalid or zero, go to IDLE state.  Invalid here just
        // means that it fails to parse, not that it is incorrect or otherwise
        // unusable.
        if(!inet_aton(dest_ip_str, &dest_ip) || dest_ip.s_addr == INADDR_ANY) {
          // Remove flow and change state to listen
          if(hashpipe_ibv_flow(hibv_ctx, 0, IBV_FLOW_SPEC_UDP,
                0, 0, 0, 0, 0, 0, 0, 0))
          {
            hashpipe_error(
                "hpguppi_pksuwl_net_thread", "hashpipe_ibv_flow error");
          }

          state = IDLE;
          // Update DAQSTATE
          hashpipe_status_lock_safe(&st);
          {
            // Update DAQSTATE
            hputs(st.buf,  "DAQSTATE", "IDLE");
          }
          hashpipe_status_unlock_safe(&st);
        }
      } // curtime != lasttime
      clock_gettime(CLOCK_MONOTONIC_RAW, &ts_stop_stat);
      elapsed_stat += ELAPSED_NS(ts_start_stat, ts_stop_stat);
      count_stat++;

      // Set status field to "waiting" if we are not getting packets
      if (!hibv_rpkt && run_threads() && waiting!=1) {
        hashpipe_status_lock_safe(&st);
        {
          hputs(st.buf, status_key, "waiting");
        }
        hashpipe_status_unlock_safe(&st);
        waiting=1;
      }

    } while (!hibv_rpkt && run_threads()); // end wait for data loop

    if(!run_threads()) {
      // We're outta here!
      if(hashpipe_ibv_release_pkts(hibv_ctx, hibv_rpkt)) {
        perror("hashpipe_ibv_release_pkts");
      }
      break;
    }

    // Got packet(s)!  Update status if needed.
    if (waiting!=0) {
      hashpipe_status_lock_safe(&st);
      {
        hputs(st.buf, status_key, "receiving");
      }
      hashpipe_status_unlock_safe(&st);
      waiting=0;
    }

    // Ignore first elapsed recv measurement as it includes wait time before
    // transmission started assuming this is run in a controlled test
    // environment where this thread is started, then blasted with packets,
    // then idle).
    if(max_recvpkt_count == 0){
      elapsed_recv = 1;
    } else {
      elapsed_recv += ELAPSED_NS(ts_start_recv, ts_stop_recv);
    }
    count_recv++;

    clock_gettime(CLOCK_MONOTONIC_RAW, &ts_start_proc);

    // For each packet: process all packets
    for(curr_rpkt = hibv_rpkt; curr_rpkt;
        //curr_rpkt = (struct hashpipe_ibv_recv_pkt *)curr_rpkt->wr.next) {
        curr_rpkt = (struct hashpipe_ibv_recv_pkt *)(((struct ibv_recv_wr *)curr_rpkt)->next)) {
      packet_count++;

      // Get pointer to vdifhdr
      vdifhdr = vdif_hdr_from_eth((void *)curr_rpkt->wr.sg_list->addr);

      // Get packet index and absolute block number for packet
      pkt_seq_num = pksuwl_get_pktidx(vdifhdr);
      pkt_blk_num = pkt_seq_num / PKTIDX_PER_BLOCK;

      // We update the status buffer at the start of each block (pol 0)
      // Also read PKTSTART, DWELL to calculate start/stop seq numbers.
      if(pkt_seq_num % PKTIDX_PER_BLOCK == 0
          && vdif_get_thread_id(vdifhdr) == 0) {
        hashpipe_status_lock_safe(&st);
        {
          hputi8(st.buf, "PKTIDX", pkt_seq_num);
          hputi8(st.buf, "PKTBLK", pkt_blk_num); // TODO do we want/need this?
          hgetu8(st.buf, "PKTSTART", &start_seq_num);
          start_seq_num -= start_seq_num % PKTIDX_PER_BLOCK;
          hputu8(st.buf, "PKTSTART", start_seq_num);
          hgetr8(st.buf, "DWELL", &dwell_seconds);
          hputr8(st.buf, "DWELL", dwell_seconds); // In case it wasn't there
          hgetr8(st.buf, "TBIN", &tbin);
          // Dwell blocks is equal to:
          //
          //       dwell_seconds
          //     ------------------
          //     tbin * ntime/block
          //
          // To get an integer number of blocks, simply truncate
          dwell_blocks = trunc(dwell_seconds
              / (tbin * PKSUWL_SAMPLES_PER_PKT * PKTIDX_PER_BLOCK));

          stop_seq_num = start_seq_num + PKTIDX_PER_BLOCK * dwell_blocks;
          hputi8(st.buf, "PKTSTOP", stop_seq_num);

          hgetu8(st.buf, "NDROP", &u64tmp);
          u64tmp += ndrop_total; ndrop_total = 0;
          hputu8(st.buf, "NDROP", u64tmp);
// TODO
#if 0
          // Calculate processing speed in Gbps (8*bytes/ns)
          hputi8(st.buf, "ELPSBITS", elapsed_bytes << 3);
          hputi8(st.buf, "ELPSNS", elapsed_ns);
          hputr4(st.buf, "NETGBPS", 8.0*elapsed_bytes / elapsed_ns);
          elapsed_bytes = 0;
          elapsed_ns = 0;
#endif
        }
        hashpipe_status_unlock_safe(&st);
      } // End status buffer block update

      // Manage blocks based on pkt_blk_num
      if(pkt_blk_num == wblk[1].block_num + 1) {
        // Finalize first working block
        finalize_block(wblk);
        // Update ndrop counter
        ndrop_total += wblk->ndrop;
        if(wblk->ndrop >= PKTIDX_PER_BLOCK) {
          // Assume one entire polrization is missing
          ndrop_total -= PKTIDX_PER_BLOCK;
        }
        // Shift working blocks
        wblk[0] = wblk[1];
        // Increment last working block
        increment_block(&wblk[1], pkt_blk_num);
        // Wait for new databuf data block to be free
        wait_for_block_free(&wblk[1], &st, status_key);
      } else if(pkt_blk_num < wblk[0].block_num - 1
      || pkt_blk_num > wblk[1].block_num + 1) {
        // Should "never" happen, so warn anbout it
        hashpipe_warn("hpguppi_pksuwl_net_thread",
            "working blocks reinit due to packet discontinuity (PKTIDX %lu)",
            pkt_seq_num);
        // Re-init working blocks and clear their data buffers
        for(wblk_idx=0; wblk_idx<2; wblk_idx++) {
          init_block_info(wblk+wblk_idx, NULL, -1, pkt_blk_num+wblk_idx);
          // Clear data buffer
          memset(block_info_data(wblk+wblk_idx), 0, PKSUWL_BLOCK_DATA_SIZE);
        }
      } else if(pkt_blk_num == wblk[0].block_num - 1) {
        // Ignore late packet, continue on to next one
        // TODO Move this check above the "once per block" status buffer
        // update (so we don't accidentally update status buffer based on a
        // late packet)?
        // Should "never" happen, so warn anbout it
        hashpipe_warn("hpguppi_pksuwl_net_thread",
            "ignoring late packet (PKTIDX %lu)",
            pkt_seq_num);
        continue;
      }

      // TODO Check START/STOP status

      // Once we get here, one of the working blocks is guaranteed to
      // correspond to this packet.  Figure out which one.
      wblk_idx = pkt_blk_num - wblk[0].block_num;

      // Copy packet data to data buffer of working block
      copy_packet_data_to_databuf(pkt_seq_num, wblk+wblk_idx, vdifhdr);

      // Count packet!
      wblk[wblk_idx].npacket++;
    } // end for each packet

    clock_gettime(CLOCK_MONOTONIC_RAW, &ts_stop_proc);

    // Release packets
    if(hashpipe_ibv_release_pkts(hibv_ctx, hibv_rpkt)) {
      perror("hashpipe_ibv_release_pkts");
    }

    // Will exit if thread has been cancelled
    pthread_testcancel();
  } // end main loop

  pthread_exit(NULL);

  return NULL;
}

static hashpipe_thread_desc_t ibverbs_thread_desc = {
    name: "hpguppi_pksuwl_net_thread",
    skey: "NETSTAT",
    init: init,
    run:  run,
    ibuf_desc: {NULL},
    obuf_desc: {hpguppi_input_databuf_create}
};

static __attribute__((constructor)) void ctor()
{
  register_hashpipe_thread(&ibverbs_thread_desc);
}

// vi: set ts=2 sw=2 et :