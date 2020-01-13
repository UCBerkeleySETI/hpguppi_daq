// hpguppi_pksuwl_net_thread.c
//
// A Hashpipe thread that receives Parkes UWL packets from a network interface.
// This thread can be compiled to use packet sockets or InfiniBand Verbs for
// packet capture.  The default is to compile for packet sockets (since they
// seem to work better than ibverbs on ConnectX-3, go figure).  To compile for
// ibverbs, add "-DUSE_IBVERBS" to CFLAGS when compiling.

// TODO TEST Wait for first (second?) start-of-block when transitioning into
//           LISTEN state so that the first block will be complete.
// TODO Add PSPKTS and PSDRPS status buffer fields for pktsock
// TODO TEST Set NETSTAE to idle in IDLE state
// TODO TEST IP_DROP_MEMBERSHIP needs mcast IP address (i.e. not 0.0.0.0)

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
#include "hpguppi_databuf.h"
#include "hpguppi_time.h"
#include "hpguppi_pksuwl.h"

#ifdef USE_IBVERBS
#include "hashpipe_ibverbs.h"
#define DEFAULT_MAX_PKT_SIZE (8400)
#define DEFAULT_SEND_PKT_NUM (1)
#define DEFAULT_RECV_PKT_NUM (16351)
#define DEFAULT_MAX_FLOWS (1)
#else
#include <net/if.h> // Fpr IFNAMSIZ
// Sizes for pktsock.  Make frame_size be a multiple of page size so that
// frames will be contiguous and blocks will be page aligned in mapped mempory.
#define PKTSOCK_BYTES_PER_FRAME (3*4096)
#define PKTSOCK_FRAMES_PER_BLOCK (1024)
#define PKTSOCK_NBLOCKS (64)
#define PKTSOCK_NFRAMES (PKTSOCK_FRAMES_PER_BLOCK * PKTSOCK_NBLOCKS)

// Adds or drops membership in a multicast group.  The `option` parameter
// must be IP_ADD_MEMBERSHIP or IP_DROP_MEMBERSHIP.  The `dst_ip` parameter
// specifies the multicast group to join.  If it is not a multicast address,
// this function does nothing so it is safe to call this function
// regardless of whether `dst_ip` is a multicast address.  Returns 0 on
// success or `errno` on error.
static int hpguppi_mcast_membership(int socket,
    const char * ifname, int option, struct in_addr * dst_ip)
{
  struct ip_mreqn mreqn;

  // Do nothing and return success if dst_ip is not multicast
  if(!(IN_MULTICAST(ntohl(dst_ip->s_addr)))) {
    return 0;
  }

  mreqn.imr_multiaddr.s_addr = dst_ip->s_addr;
  mreqn.imr_address.s_addr = INADDR_ANY;
  if((mreqn.imr_ifindex = if_nametoindex(ifname)) == 0) {
    return errno;
  }

  if(setsockopt(socket, IPPROTO_IP, option, &mreqn, sizeof(mreqn))) {
    return errno;
  }

  return 0;
}
#endif // USE_IBVERBS

#define HPGUPPI_DAQ_CONTROL "/tmp/hpguppi_daq_control"
#define MAX_CMD_LEN 1024

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
  bi->ndrop = (2 /*pols*/ * PKSUWL_PKTIDX_PER_BLOCK) - bi->npacket;
  sprintf(dropstat, "%d/%d", bi->ndrop, (2 /*pols*/ * PKSUWL_PKTIDX_PER_BLOCK));
  hputi8(header, "PKTIDX", bi->block_num * PKSUWL_PKTIDX_PER_BLOCK);
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
  char netstat[80];
  char netbuf_status[80];
  int netbuf_full = hpguppi_input_databuf_total_status(bi->db);
  sprintf(netbuf_status, "%d/%d", netbuf_full, bi->db->header.n_block);
  hashpipe_status_lock_safe(st);
  hgets(st->buf, status_key, sizeof(netstat), netstat);
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
  hputs(st->buf, status_key, netstat);
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
// data values must be converted to two's complement form.
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
  off_t offset = (off_t)(packet_idx % PKSUWL_PKTIDX_PER_BLOCK);
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

// Check the given pktidx value against the status buffer's PKTSTART/PKTSTOP
// values. Logic goes something like this:
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
enum run_states check_start_stop(hashpipe_status_t *st, uint64_t pktidx)
{
  enum run_states retval = LISTEN;
  uint32_t sttvalid = 0;
  uint64_t pktstart = 0;
  uint64_t pktstop = 0;

  struct timeval tv;

  int    stt_imjd = 0;
  int    stt_smjd = 0;
  double stt_offs = 0;

  hashpipe_status_lock_safe(st);
  {
    hgetu4(st->buf, "STTVALID", &sttvalid);
    hgetu8(st->buf, "PKTSTART", &pktstart);
    hgetu8(st->buf, "PKTSTOP", &pktstop);

    if(pktstart <= pktidx && pktidx < pktstop) {
      retval = RECORD;
      hputs(st->buf, "DAQSTATE", "RECORD");

      if(sttvalid != 1) {
        hputu4(st->buf, "STTVALID", 1);
        // Calc IMJD/SMJD/OFFS
        pksuwl_pktidx_to_timeval(pktstart, &tv);
        get_mjd_from_timeval(&tv, &stt_imjd, &stt_smjd, &stt_offs);
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
#ifdef USE_IBVERBS
  struct hashpipe_ibv_context hibv_ctx;
#else
  struct hashpipe_pktsock ps;
  struct in_addr mcast_group;
#endif // USE_IBVERBS
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
  const char * status_key = args->thread_desc->skey;

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
    hputi4(st.buf, "NDROP", 0);
#ifndef USE_IBVERBS
    hputu8(st.buf, "PSPKTS", 0);
    hputu8(st.buf, "PSDRPS", 0);
#endif // !USE_IBVERBS
    // Set status_key to init
    hputs(st.buf, status_key, "init");
  }
  hashpipe_status_unlock_safe(&st);

#ifndef USE_IBVERBS
  // Set up pktsock
  net_params->ps.frame_size = PKTSOCK_BYTES_PER_FRAME;
  // total number of frames
  net_params->ps.nframes = PKTSOCK_NFRAMES;
  // number of blocks
  net_params->ps.nblocks = PKTSOCK_NBLOCKS;

  rv = hashpipe_pktsock_open(
      &net_params->ps, net_params->ifname, PACKET_RX_RING);
  if (rv!=HASHPIPE_OK) {
      hashpipe_error("hpguppi_net_thread", "Error opening pktsock.");
      pthread_exit(NULL);
  }
#endif // !USE_IBVERBS

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
#ifdef USE_IBVERBS
  struct hashpipe_ibv_context * hibv_ctx = &net_params->hibv_ctx;
#else
  struct hashpipe_pktsock * p_ps = &net_params->ps;
  // An error return value will cause a later error if/when used
  int mcast_subscriber = socket(AF_INET, SOCK_DGRAM, 0);
#endif // USE_IBVERBS

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
  unsigned waiting = 0;
  // Update status_key with idle state
  hashpipe_status_lock_safe(&st);
  {
    hputs(st.buf, status_key, "idle");
  }
  hashpipe_status_unlock_safe(&st);

  // Misc counters, etc
  int rv;
  // String version of destination address
  char dest_ip_str[80] = {};
  // Numeric form of dest_ip
  struct in_addr dest_ip;

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
#ifdef USE_IBVERBS
  struct hashpipe_ibv_recv_pkt * hibv_rpkt = NULL;
  struct hashpipe_ibv_recv_pkt * curr_rpkt;
#else
  unsigned char *p_frame;
  unsigned int pspkts = 0;
  unsigned int psdrps = 0;
#endif // USE_IBVERBS
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

  // Initialize working blocks
  for(wblk_idx=0; wblk_idx<2; wblk_idx++) {
    init_block_info(wblk+wblk_idx, db, wblk_idx, wblk_idx);
    wait_for_block_free(wblk+wblk_idx, &st, status_key);
  }

#ifdef USE_IBVERBS
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
#else
  // Drop all pktsock packets to date
  while((p_frame=hashpipe_pktsock_recv_frame_nonblock(p_ps))) {
      hashpipe_pktsock_release_frame(p_frame);
  }
  // Reset packet socket counters
  hashpipe_pktsock_stats(p_ps, &pspkts, &psdrps);
#endif // USE_IBVERBS

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
#ifdef USE_IBVERBS
        // Add flow and change state to listen
        if(hashpipe_ibv_flow(hibv_ctx, 0, IBV_FLOW_SPEC_UDP,
              hibv_ctx->mac, NULL, 0, 0,
              0, ntohl(dest_ip.s_addr), 0, net_params->port))
        {
          hashpipe_error(
              "hpguppi_pksuwl_net_thread", "hashpipe_ibv_flow error");
          // Stay in IDLE state loop
          continue;
        }
#else
        // If multicast address
        if((IN_MULTICAST(ntohl(dest_ip.s_addr)))) {
          // IP_ADD_MEMBERSHIP
          if(hpguppi_mcast_membership(mcast_subscriber,
                net_params->ifname, IP_ADD_MEMBERSHIP, &dest_ip)) {
            hashpipe_error("hpguppi_pksuwl_net_thread",
                "could not add mcast membership for group %s", dest_ip_str);
            // Stay in IDLE state loop
            continue;
          }
          // Remember multicast group
          net_params->mcast_group.s_addr = dest_ip.s_addr;
        }
#endif // USE_IBVERBS

        // Transition to LISTEN state and start waiting for packets
        state = LISTEN;
        waiting = 1;
        // Update DAQSTATE and status_key
        hashpipe_status_lock_safe(&st);
        {
          hputs(st.buf, "DAQSTATE", "LISTEN");
          hputs(st.buf, status_key, "waiting");
        }
        hashpipe_status_unlock_safe(&st);
      }
    } // end while state == IDLE

    // Wait for data
    do {
      clock_gettime(CLOCK_MONOTONIC_RAW, &ts_start_recv);
#ifdef USE_IBVERBS
#define GOT_PACKET (hibv_rpkt)
      hibv_rpkt = hashpipe_ibv_recv_pkts(hibv_ctx, 1000); // 1 second timeout
#else
#define GOT_PACKET (p_frame)
      p_frame = hashpipe_pktsock_recv_udp_frame(
          p_ps, net_params->port, 1000); // 1 second timeout
#endif // USE_IBVERBS
      clock_gettime(CLOCK_MONOTONIC_RAW, &ts_stop_recv);

      if(!GOT_PACKET) {
        if(errno) {
#ifdef USE_IBVERBS
          perror("hashpipe_ibv_recv_pkts");
#else
          perror("hashpipe_pktsock_recv_udp_frame");
#endif // USE_IBVERBS
          errno = 0;
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
#ifdef USE_IBVERBS
          // Remove flow and change state to listen
          if(hashpipe_ibv_flow(hibv_ctx, 0, IBV_FLOW_SPEC_UDP,
                0, 0, 0, 0, 0, 0, 0, 0))
          {
            hashpipe_error(
                "hpguppi_pksuwl_net_thread", "hashpipe_ibv_flow error");
          }
#else
          // IP_DROP_MEMBERSHIP
          if(hpguppi_mcast_membership(mcast_subscriber,
                net_params->ifname, IP_DROP_MEMBERSHIP, &net_params->mcast_group)) {
            hashpipe_warn("hpguppi_pksuwl_net_thread",
                "could not add mcast membership for group %.8x",
                ntohl(net_params->mcast_group.s_addr));
          }
          // Forget mcast_group
          net_params->mcast_group.s_addr = INADDR_ANY;
#endif // USE_IBVERBS

          // Switch to IDLE state (and ensure waiting flag is clear)
          state = IDLE;
          waiting = 0;
          // Update DAQSTATE and status_key
          hashpipe_status_lock_safe(&st);
          {
            hputs(st.buf, "DAQSTATE", "IDLE");
            hputs(st.buf, status_key, "idle");
          }
          hashpipe_status_unlock_safe(&st);
        }
      } // curtime != lasttime
      clock_gettime(CLOCK_MONOTONIC_RAW, &ts_stop_stat);
      elapsed_stat += ELAPSED_NS(ts_start_stat, ts_stop_stat);
      count_stat++;

      // Set status field to "waiting" if we are not getting packets
      if (!GOT_PACKET && run_threads() && state != IDLE && !waiting) {
        hashpipe_status_lock_safe(&st);
        {
          hputs(st.buf, status_key, "waiting");
        }
        hashpipe_status_unlock_safe(&st);
        waiting=1;
      }

    } while (!GOT_PACKET && run_threads() && state != IDLE); // end wait for data loop

    if(!run_threads()) {
      // We're outta here!
#ifdef USE_IBVERBS
      if(hashpipe_ibv_release_pkts(hibv_ctx, hibv_rpkt)) {
        perror("hashpipe_ibv_release_pkts");
      }
#else
      hashpipe_pktsock_release_frame(p_frame);
#endif // USE_IBVERBS
      break;
    } else if(state == IDLE) {
      // Go back to top of main loop
      continue;
    }

    // Got packet(s)!  Update status if needed.
    if (waiting) {
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

#ifdef USE_IBVERBS
    // For each packet: process all packets
    for(curr_rpkt = hibv_rpkt; curr_rpkt;
        //curr_rpkt = (struct hashpipe_ibv_recv_pkt *)curr_rpkt->wr.next) {
        curr_rpkt = (struct hashpipe_ibv_recv_pkt *)(((struct ibv_recv_wr *)curr_rpkt)->next)) {
      // Get pointer to vdifhdr
      vdifhdr = vdif_hdr_from_eth((void *)curr_rpkt->wr.sg_list->addr);
#else
      // Get pointer to vdifhdr
      vdifhdr = (struct vdifhdr *)(PKT_UDP_DATA(p_frame));
#endif // USE_IBVERBS

      // Count packet
      packet_count++;

      // Get packet index and absolute block number for packet
      pkt_seq_num = pksuwl_get_pktidx(vdifhdr);
      pkt_blk_num = pkt_seq_num / PKSUWL_PKTIDX_PER_BLOCK;

      // We update the status buffer at the start of each block (pol 0)
      // Also read PKTSTART, DWELL to calculate start/stop seq numbers.
      if(pkt_seq_num % PKSUWL_PKTIDX_PER_BLOCK == 0
          && vdif_get_thread_id(vdifhdr) == 0) {
        hashpipe_status_lock_safe(&st);
        {
          hputi8(st.buf, "PKTIDX", pkt_seq_num);
          hputi8(st.buf, "PKTBLK", pkt_blk_num); // TODO do we want/need this?
          hgetu8(st.buf, "PKTSTART", &start_seq_num);
          start_seq_num -= start_seq_num % PKSUWL_PKTIDX_PER_BLOCK;
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
              / (tbin * PKSUWL_SAMPLES_PER_PKT * PKSUWL_PKTIDX_PER_BLOCK));

          stop_seq_num = start_seq_num + PKSUWL_PKTIDX_PER_BLOCK * dwell_blocks;
          hputi8(st.buf, "PKTSTOP", stop_seq_num);

          hgetu8(st.buf, "NDROP", &u64tmp);
          u64tmp += ndrop_total; ndrop_total = 0;
          hputu8(st.buf, "NDROP", u64tmp);

#ifndef USE_IBVERBS
          // Update PSPKTS and PSDRPS
          hashpipe_pktsock_stats(p_ps, &pspkts, &psdrps);

          hgetu8(st.buf, "PSPKTS", &u64tmp);
          u64tmp += pspkts;
          hputu8(st.buf, "PSPKTS", u64tmp);

          hgetu8(st.buf, "PSDRPS", &u64tmp);
          u64tmp += psdrps;
          hputu8(st.buf, "PSDRPS", u64tmp);
#endif // !USE_IBVERBS

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
        if(wblk->ndrop >= PKSUWL_PKTIDX_PER_BLOCK) {
          // Assume one entire polrization is missing
          ndrop_total -= PKSUWL_PKTIDX_PER_BLOCK;
        }
        // Shift working blocks
        wblk[0] = wblk[1];
        // Check start/stop using wblk[0]'s first PKTIDX
        state = check_start_stop(&st, wblk[0].block_num * PKSUWL_PKTIDX_PER_BLOCK);
        // Increment last working block
        increment_block(&wblk[1], pkt_blk_num);
        // Wait for new databuf data block to be free
        wait_for_block_free(&wblk[1], &st, status_key);
      }
      // Check for PKTIDX discontinuity
      else if(pkt_blk_num < wblk[0].block_num - 1
      || pkt_blk_num > wblk[1].block_num + 1) {
        // Should only happen when transitioning into LISTEN, so warn about it
        hashpipe_warn("hpguppi_pksuwl_net_thread",
            "working blocks reinit due to packet discontinuity (PKTIDX %lu)",
            pkt_seq_num);
        // Re-init working blocks for next block number
        // and clear their data buffers
        for(wblk_idx=0; wblk_idx<2; wblk_idx++) {
          init_block_info(wblk+wblk_idx, NULL, -1, pkt_blk_num+wblk_idx+1);
          // Clear data buffer
          memset(block_info_data(wblk+wblk_idx), 0, PKSUWL_BLOCK_DATA_SIZE);
        }

        // Check start/stop using wblk[0]'s first PKTIDX
        state = check_start_stop(&st, wblk[0].block_num * PKSUWL_PKTIDX_PER_BLOCK);
#if 0
// This happens after discontinuities (e.g. on startup), so don't warn about
// it.
      } else if(pkt_blk_num == wblk[0].block_num - 1) {
        // Ignore late packet, continue on to next one
        // TODO Move this check above the "once per block" status buffer
        // update (so we don't accidentally update status buffer based on a
        // late packet)?
        // Should "never" happen, so warn anbout it
        hashpipe_warn("hpguppi_pksuwl_net_thread",
            "ignoring late packet (PKTIDX %lu)",
            pkt_seq_num);
#endif
      }

      // Once we get here, compute the index of the working block corresponding
      // to this packet.  The computed index may not correspond to a valid
      // working block!
      wblk_idx = pkt_blk_num - wblk[0].block_num;

      // Only copy packet data and count packet if its wblk_idx is valid
      if(0 <= wblk_idx && wblk_idx < 2) {
        // Copy packet data to data buffer of working block
        copy_packet_data_to_databuf(pkt_seq_num, wblk+wblk_idx, vdifhdr);

        // Count packet for block
        wblk[wblk_idx].npacket++;
      }

#ifdef USE_IBVERBS
    } // end for each packet
#endif // USE_IBVERBS

    clock_gettime(CLOCK_MONOTONIC_RAW, &ts_stop_proc);

    // Release packets
#ifdef USE_IBVERBS
    if(hashpipe_ibv_release_pkts(hibv_ctx, hibv_rpkt)) {
      perror("hashpipe_ibv_release_pkts");
    }
#else
    hashpipe_pktsock_release_frame(p_frame);
#endif // USE_IBVERBS

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
