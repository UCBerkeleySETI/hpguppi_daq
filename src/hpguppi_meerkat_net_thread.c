// hpguppi_meerkat_net_thread.c
//
// A Hashpipe thread that receives MeerKAT SPEAD packets from a network
// interface.  This thread can be compiled to use packet sockets or InfiniBand
// Verbs for packet capture.  The default is to compile for packet sockets
// (since they seem to work better than ibverbs on ConnectX-3, go figure).  To
// compile for ibverbs, add "-DUSE_IBVERBS" to CFLAGS when compiling.  Note
// that value assigned to USE_IBVERBS is not significant. "-DUSE_IBVERBS=0" has
// the same effect as "-DUSE_IBVERBS=1".
#if 1
#define USE_IBVERBS
#else
#ifdef USE_IBVERBS
#undef USE_IBVERBS
#endif
#endif

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

#include <immintrin.h>

#include "hashpipe.h"
#include "hpguppi_databuf.h"
#include "hpguppi_time.h"
#include "hpguppi_mkfeng.h"

#ifdef USE_IBVERBS
#include "hashpipe_ibverbs.h"
#define DEFAULT_MAX_PKT_SIZE (8400)
#define DEFAULT_SEND_PKT_NUM (1)
#define DEFAULT_RECV_PKT_NUM (16351)
#define DEFAULT_MAX_FLOWS (16)
#else
#include <net/if.h> // Fpr IFNAMSIZ
// Sizes for pktsock.  Make frame_size be a multiple of page size so that
// frames will be contiguous and blocks will be page aligned in mapped mempory.
#if 0
#define PKTSOCK_BYTES_PER_FRAME (3*4096)
#define PKTSOCK_FRAMES_PER_BLOCK (1024)
#define PKTSOCK_NBLOCKS (64)
#else
#define PKTSOCK_BYTES_PER_FRAME (4*4096)
#define PKTSOCK_FRAMES_PER_BLOCK (8)
#define PKTSOCK_NBLOCKS (800*12)
#endif
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
// (i.e. whenever PKTIDX is a multiple of pktidx_per_block), the value
// for PKTSTART and DWELL are read from the status buffer.  PKTSTART is rounded
// down, if needed, to ensure that it is a multiple of pktidx_per_block,
// then PKTSTART is written back to the status buffer.  DWELL is interpreted as
// the number of seconds to record and is used to calculate PKTSTOP (which gets
// rounded down, if needed, to be a multiple of pktidx_per_block).
//
// The IDLE state is entered when there is no DESTIP defined in the status
// buffer or it is 0.0.0.0.  In the IDLE state, the DESTIP value in the status
// buffer is checked once per second.  If it is found to be something other
// than 0.0.0.0, the state transitions to the LISTEN state and the current
// blocks are reinitialized.
//
// To be operationally compatible with other hpguppi net threads, a "command
// FIFO" is created and read from in all states, but commands sent there are
// ignored.  State transitions are controlled entirely by DESTIP and
// PKTSTART/DWELL status buffer fields.
//
// In the LISTEN state, incoming packets are processed (i.e. stored in the net
// thread's output buffer) and full blocks are passed to the next thread.  When
// the processed PKTIDX is equal to PKTSTART the state transitions to RECORD
// and the following actions occur:
//
//   1. The MJD of the observation start time is calculated from PKTIDX,
//      SYNCTIME, and other parameters.
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
// The PKTSTART/PKTSTOP tests are done every time the work blocks are advanced.
//
// The downstream thread (i.e. hpguppi_rawdisk_thread) is expected to use a
// combination of PKTIDX, PKTSTART, PKTSTOP, and (optionally) STTVALID to
// determine whether the blocks should be discarded or processed (e.g. written
// to disk).

enum run_states {IDLE, LISTEN, RECORD};

#if 0
// Non-temporal (cache bypass) clearing of memory
static
void clear_memory(void * dst, size_t size)
{
  // Create 256-bit (32-byte) zero value
  const __m256i m256 = _mm256_setzero_si256();

  // Cast dst to __m256i pointer
  __m256i * p256 = (__m256i *)dst;

  // Convert size from 1 byte units to 32 byte units
  size >>= 5;

  // While size > 0
  while(size) {
    *p256++ = m256;
    size--;
  }
}
#endif

// Structure related to block management
struct block_info {
  // Set at start of run
  struct hpguppi_input_databuf *db; // Pointer to overall shared mem databuf
  // Set at start of block
  int block_idx;                    // Block index number in databuf
  int64_t block_num;                // Absolute block number
  uint64_t pktidx_per_block;
  uint64_t pkts_per_block;
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
// bi->pkts_per_block is set of pkt_size > 0.
static void init_block_info(struct block_info *bi,
    struct hpguppi_input_databuf *db, int block_idx, int64_t block_num,
    uint64_t pkts_per_block)
{
  if(db) {
    bi->db = db;
  }
  if(block_idx >= 0) {
    bi->block_idx = block_idx;
  }
  bi->block_num = block_num;
  if(pkts_per_block > 0) {
    bi->pkts_per_block = pkts_per_block;
  }
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
  if(bi->pkts_per_block > bi->npacket) {
    bi->ndrop = bi->pkts_per_block - bi->npacket;
  }
#if 0
if(bi->pkts_per_block != bi->npacket) {
  printf("pktidx %ld pkperblk %ld npkt %u\n",
    bi->block_num * bi->pktidx_per_block,
    bi->pkts_per_block, bi->npacket);
}
#endif
  sprintf(dropstat, "%d/%lu", bi->ndrop, bi->pkts_per_block);
  hputi8(header, "PKTIDX", bi->block_num * bi->pktidx_per_block);
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
static void increment_block(struct block_info *bi, int64_t block_num)
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
      hashpipe_error("hpguppi_meerkat_net_thread",
          "error waiting for free databuf");
      pthread_exit(NULL);
    }
  }
  hashpipe_status_lock_safe(st);
  hputs(st->buf, status_key, netstat);
  memcpy(block_info_header(bi), st->buf, HASHPIPE_STATUS_TOTAL_SIZE);
  hashpipe_status_unlock_safe(st);

#if 0
  // TODO Move this out of net thread (takes too long)
  // TODO Just clear effective block size?
  //memset(block_info_data(bi), 0, BLOCK_DATA_SIZE);
  clear_memory(block_info_data(bi), BLOCK_DATA_SIZE);
#endif
}

#if 0 // debug(1) vs real(0) copy
struct ts_mk_feng_spead_info {
  struct timespec ts;
  struct mk_feng_spead_info fesi;
};

struct debug_data_block {
  uint64_t npkts;
  struct ts_mk_feng_spead_info ts_fesi[];
};

// Append spead header info to data block
static void copy_packet_data_to_databuf(struct block_info *bi,
    const struct mk_obs_info * p_oi,
    const struct mk_feng_spead_info * p_fesi,
    uint8_t * p_spead_payload)
{
  // Get pointer to data block (cast as a debug_data_block)
  struct debug_data_block * ddb =
    (struct debug_data_block *)block_info_data(bi);

  // Get pointer to next ts_mk_feng_spead_info element
  struct ts_mk_feng_spead_info * ts_fesi = &(ddb->ts_fesi[ddb->npkts]);

  // Store timestamp
  clock_gettime(CLOCK_MONOTONIC_RAW, &ts_fesi->ts);

  // Copy mk_feng_spead_info
  memcpy(&ts_fesi->fesi, p_fesi, sizeof(struct mk_feng_spead_info));

  // Increment packet counter
  ddb->npkts++;
}
#else
// The copy_packet_data_to_databuf() function does what it says: copies packet
// data into a data buffer.
//
// The data buffer block is identified by the block_info structure pointed to
// by the bi parameter.
//
// The p_oi parameter points to the observation's obs_info data.
//
// The p_fesi parameter points to an mk_feng_spead_info packet containing the
// packet's spead metadata.
//
// The p_spead_payload parameter points to the spead payload of the packet.
static void copy_packet_data_to_databuf(struct block_info *bi,
    const struct mk_obs_info * p_oi,
    const struct mk_feng_spead_info * p_fesi,
    uint8_t * p_spead_payload)
{
  uint8_t * src = p_spead_payload;
  uint8_t * dst = (uint8_t *)block_info_data(bi);
  int bytes_to_copy = p_fesi->payload_size;

  // istride is the size of a HNTIME samples in bytes
  size_t istride = 4 * p_oi->hntime;

  // ostride is the size of a "row" in bytes
  size_t ostride = 4 * mk_ntime(BLOCK_DATA_SIZE, *p_oi);

  // slot_idx is the index of the slot in the block where the packet's heap goes
  int slot_idx = mk_pktidx(*p_oi, *p_fesi) % bi->pktidx_per_block;

  // block_chan is the "row" in the data block where this packet's heap starts
  int block_chan = mk_block_chan(*p_oi, *p_fesi);

#if 0
printf("feng_id    = %lu\n", p_fesi->feng_id);
printf("nstrm      = %d\n", p_oi->nstrm);
printf("hnchan     = %d\n", p_oi->hnchan);
printf("feng_chan  = %lu\n", p_fesi->feng_chan);
printf("schan      = %d\n", p_oi->schan);
printf("b_to_copy  = %d\n", bytes_to_copy);
printf("istride    = 0x%08lx\n", istride);
printf("ostride    = 0x%08lx\n", ostride);
printf("slot_idx   = %d\n", slot_idx);
printf("block_chan = %d\n", block_chan);
printf("dst        = 0x%p\n", dst);
#endif

  // Advance dst to start of slot
  dst += slot_idx * istride;
#if 0
printf("dst        = 0x%p\n", dst);
#endif

  // Advance dst to start of heap
  dst += block_chan * ostride;
#if 0
printf("dst        = 0x%p\n", dst);
#endif

  // Advance dst to heap offset.
  // For now assume that packets are hntime aligned within heap.
  dst += (p_fesi->heap_offset / istride) * ostride;
#if 0
printf("dst        = 0x%p\n", dst);
printf("\n");
#endif

  // Copy samples
  // TODO Ensure that bytes_to_copy is a multiple of istride.
  while(bytes_to_copy > 0) {
    memcpy(dst, src, istride);
    src += istride;
    dst += ostride;
    bytes_to_copy -= istride;
  }
}
#endif // debug vs real copy

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

  uint64_t hclocks = 1;
  uint32_t fenchan = 1;
  uint64_t synctime = 0;
  double chan_bw = 1.0;

  double realtime_secs = 0.0;
  struct timespec ts;

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

        hgetu8(st->buf, "HCLOCKS", &hclocks);
        hgetu4(st->buf, "FENCHAN", &fenchan);
        hgetr8(st->buf, "CHAN_BW", &chan_bw);
        hgetu8(st->buf, "SYNCTIME", &synctime);

        // Calc real-time seconds since SYNCTIME for pktidx:
        //
        //                        pktidx * hclocks
        //     realtime_secs = -----------------------
        //                      2e6 * fenchan * chan_bw
        if(fenchan * chan_bw != 0.0) {
          realtime_secs = (pktidx * hclocks) / (2e6 * fenchan * fabs(chan_bw));
        }

        ts.tv_sec = (time_t)(synctime + rint(realtime_secs));
        ts.tv_nsec = (long)((realtime_secs - rint(realtime_secs)) * 1e9);

        get_mjd_from_timespec(&ts, &stt_imjd, &stt_smjd, &stt_offs);

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
  int blocsize=BLOCK_DATA_SIZE;
  int directio=1;
  int nbits=8;
  int npol=4;
  double obsfreq=0;
  double obsbw=128.0;
  double chan_bw=1.0;
  int obsnchan=1;
  int nants=1;
  int overlap=0;
  double tbin=1e-6;
  char obs_mode[80];
  char dest_ip[80];
  char fifo_name[PATH_MAX];
  struct net_params * net_params;
  const char * status_key = args->thread_desc->skey;

  // Create control FIFO (/tmp/hpguppi_daq_control/$inst_id)
  int rv = mkdir(HPGUPPI_DAQ_CONTROL, 0777);
  if (rv!=0 && errno!=EEXIST) {
    hashpipe_error("hpguppi_meerkat_net_thread", "Error creating control fifo directory");
    return HASHPIPE_ERR_SYS;
  } else if(errno == EEXIST) {
    errno = 0;
  }

  sprintf(fifo_name, "%s/%d", HPGUPPI_DAQ_CONTROL, args->instance_id);
  rv = mkfifo(fifo_name, 0666);
  if (rv!=0 && errno!=EEXIST) {
    hashpipe_error("hpguppi_meerkat_net_thread", "Error creating control fifo");
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
  net_params->port = 7148;

  hashpipe_status_t st = args->st;

  hashpipe_status_lock_safe(&st);
  {
    // Get info from status buffer if present (no change if not present)
    hgets(st.buf,  "BINDHOST", sizeof(net_params->ifname), net_params->ifname);
    hgeti4(st.buf, "BINDPORT", &net_params->port);
    hgeti4(st.buf, "BLOCSIZE", &blocsize);
    hgeti4(st.buf, "DIRECTIO", &directio);
    hgeti4(st.buf, "NANTS", &nants);
    hgeti4(st.buf, "NBITS", &nbits);
    hgeti4(st.buf, "NPOL", &npol);
    hgetr8(st.buf, "OBSFERQ", &obsfreq);
    hgetr8(st.buf, "OBSBW", &obsbw);
    hgetr8(st.buf, "CHAN_BW", &chan_bw);
    hgeti4(st.buf, "OBSNCHAN", &obsnchan);
    hgeti4(st.buf, "OVERLAP", &overlap);
    hgets(st.buf, "OBS_MODE", 80, obs_mode);

    // Prevent div-by-zero errors (should never happen...)
    if(nants == 0) {
      nants = 1;
      hputi4(st.buf, "NANTS", nants);
    }

    // If CHAN_BW is zero, set to default value (1 MHz)
    if(chan_bw == 0.0) {
      chan_bw = 1.0;
    }

    // Calculate tbin and obsbw from chan_bw
    tbin = 1e-6 / fabs(chan_bw);
    obsbw = chan_bw * obsnchan / nants;

    // Store bind host/port info etc in status buffer (in case it was not there
    // before).
    hputs(st.buf, "BINDHOST", net_params->ifname);
    hputi4(st.buf, "BINDPORT", net_params->port);
    hputi4(st.buf, "BLOCSIZE", blocsize);
    hputi4(st.buf, "DIRECTIO", directio);
    hputi4(st.buf, "NBITS", nbits);
    hputi4(st.buf, "NPOL", npol);
    hputr8(st.buf, "OBSBW", obsbw);
    hputr8(st.buf, "CHAN_BW", chan_bw);
    hputi4(st.buf, "OBSNCHAN", obsnchan);
    hputi4(st.buf, "OVERLAP", overlap);
    // Force PKTFMT to be "SPEAD"
    hputs(st.buf, "PKTFMT", "SPEAD");
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
#if 0
int debug_i=0, debug_j=0;
#endif
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
  sprintf(fifo_name, "%s/%d", HPGUPPI_DAQ_CONTROL, args->instance_id);
  int fifo_fd = open(fifo_name, O_RDONLY | O_NONBLOCK);
  if (fifo_fd<0) {
      hashpipe_error("hpguppi_meerkat_net_thread", "Error opening control fifo)");
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
  char * pchar;
  // Numeric form of dest_ip
  struct in_addr dest_ip;
#ifndef USE_IBVERBS
  struct in_addr dest_ip_tmp;
#endif
  int dest_idx;
  int nstreams;

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
  uint64_t pkt_seq_num = 0;
  int64_t pkt_blk_num = 0; // Signed to avoid problems comparing with -1
  uint64_t start_seq_num=0;
  uint64_t stop_seq_num=0;
  uint64_t status_seq_num;
  //uint64_t last_seq_num=2048;
  //uint64_t nextblock_seq_num=0;
  uint64_t dwell_blocks = 0;
  double dwell_seconds = 300.0;
  double chan_bw = 1.0;
  double tbin = 1.0e-6;

  // Heartbeat variables
  time_t lasttime = 0;
  time_t curtime = 0;
  char timestr[32] = {0};

  // Variables for counting packets and bytes.
  uint64_t packet_count = 0; // Counts packets between updates to status buffer
  uint64_t u64tmp = 0; // Used for status buffer interactions
  uint64_t max_recvpkt_count = 0;
  uint64_t ndrop_total = 0;
  uint64_t nlate = 0;

  // Variables for handing received packets
#ifdef USE_IBVERBS
  struct hashpipe_ibv_recv_pkt * hibv_rpkt = NULL;
  struct hashpipe_ibv_recv_pkt * curr_rpkt;
#else
  unsigned char *p_frame;
  unsigned int pspkts = 0;
  unsigned int psdrps = 0;
#endif // USE_IBVERBS
  struct udppkt * p_udppkt;
  uint8_t * p_spead_payload = NULL;

  // Structure to hold observation info, init all fields to invalid values
  struct mk_obs_info obs_info;
  mk_obs_info_init(&obs_info);

  // OBSNCHAN is total number of channels handled by this instance.
  // For arrays like MeerKAT, it is NANTS*NSTRM*HNCHAN.
  int obsnchan = 1;
  // PKTIDX per block (depends on obs_info).  Init to 0 to cause div-by-zero
  // error if using it unintialized (crash early, crash hard!).
  uint32_t pktidx_per_block = 0;
  // Effective block size (will be less than BLOCK_DATA_SIZE when
  // BLOCK_DATA_SIZE is not divisible by NCHAN and/or HNTIME.
  // Historically, BLOCSIZE gets stored as a signed 4 byte integer
  int32_t eff_block_size;

  // Structure to hold feng spead info from packet
  struct mk_feng_spead_info feng_spead_info = {0};

  // Variables for tracking timing stats
  struct timespec ts_start_recv, ts_stop_recv;
  uint64_t elapsed_recv = 0;
  uint64_t count_recv = 0;
  struct timespec ts_start_stat, ts_stop_stat;
  uint64_t elapsed_stat = 0;
  uint64_t count_stat = 0;
  struct timespec ts_start_proc, ts_stop_proc;
  uint64_t elapsed_proc = 0;
  uint64_t count_proc = 0;

  // Initialize working blocks
  for(wblk_idx=0; wblk_idx<2; wblk_idx++) {
    init_block_info(wblk+wblk_idx, db, wblk_idx, wblk_idx, 0);
    wait_for_block_free(wblk+wblk_idx, &st, status_key);
  }

  // Get any obs info from status buffer, store values
  hashpipe_status_lock_safe(&st);
  {
    // Read (no change if not present)
    hgetu4(st.buf, "FENCHAN", &obs_info.fenchan);
    hgetu4(st.buf, "NANTS",   &obs_info.nants);
    hgetu4(st.buf, "NSTRM",   &obs_info.nstrm);
    hgetu4(st.buf, "HNTIME",  &obs_info.hntime);
    hgetu4(st.buf, "HNCHAN",  &obs_info.hnchan);
    hgetu8(st.buf, "HCLOCKS", &obs_info.hclocks);
    hgeti4(st.buf, "SCHAN",   &obs_info.schan);

    // If obs_info is valid
    if(mk_obs_info_valid(obs_info)) {
      // Update obsnchan, pktidx_per_block, and eff_block_size
      obsnchan = mk_obsnchan(obs_info);
      pktidx_per_block = mk_pktidx_per_block(BLOCK_DATA_SIZE, obs_info);
      eff_block_size = mk_block_size(BLOCK_DATA_SIZE, obs_info);
    }

    // Write (store default/invlid values if not present)
    hputu4(st.buf, "FENCHAN", obs_info.fenchan);
    hputu4(st.buf, "NANTS",   obs_info.nants);
    hputu4(st.buf, "NSTRM",   obs_info.nstrm);
    hputu4(st.buf, "HNTIME",  obs_info.hntime);
    hputu4(st.buf, "HNCHAN",  obs_info.hnchan);
    hputu8(st.buf, "HCLOCKS", obs_info.hclocks);
    hputi4(st.buf, "SCHAN",   obs_info.schan);

    hputu4(st.buf, "OBSNCHAN", obsnchan);
    hputu4(st.buf, "PIPERBLK", pktidx_per_block);
    hputi4(st.buf, "BLOCSIZE", eff_block_size);
  }
  hashpipe_status_unlock_safe(&st);

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
    hashpipe_error("hpguppi_meerkat_net_thread", "Error initializing ibverbs.");
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
          hashpipe_warn("hpguppi_meerkat_net_thread",
              "ignoring %s command", fifo_cmd);
        }
      } else if(rv < 0) {
        hashpipe_error("hpguppi_meerkat_net_thread", "command fifo poll error");
        // Bail out (this should "never" happen)
        break;
      }

      // We perform some status buffer updates every second
      time(&curtime);
      if(curtime != lasttime) {
        ctime_r(&curtime, timestr);
        timestr[strlen(timestr)-1] = '\0'; // Chop off trailing newline
      }

      // Check dest_ip/port in status buffer, get obs info parameters, and, if
      // needed, update DAQPULSE
      hashpipe_status_lock_safe(&st);
      {
        // Get DESTIP address and BINDPORT (a historical misnomer for DESTPORT)
        hgets(st.buf,  "DESTIP", sizeof(dest_ip_str), dest_ip_str);
        hgeti4(st.buf,  "BINDPORT", &net_params->port);

        hgetu4(st.buf, "FENCHAN", &obs_info.fenchan);
        hgetu4(st.buf, "NANTS",   &obs_info.nants);
        hgetu4(st.buf, "NSTRM",   &obs_info.nstrm);
        hgetu4(st.buf, "HNTIME",  &obs_info.hntime);
        hgetu4(st.buf, "HNCHAN",  &obs_info.hnchan);
        hgetu8(st.buf, "HCLOCKS", &obs_info.hclocks);
        hgeti4(st.buf, "SCHAN",   &obs_info.schan);

        // If obs_info is valid
        if(mk_obs_info_valid(obs_info)) {
          // Update obsnchan, pktidx_per_block, and eff_block_size
          obsnchan = mk_obsnchan(obs_info);
          pktidx_per_block = mk_pktidx_per_block(BLOCK_DATA_SIZE, obs_info);
          eff_block_size = mk_block_size(BLOCK_DATA_SIZE, obs_info);

          hputu4(st.buf, "OBSNCHAN", obsnchan);
          hputu4(st.buf, "PIPERBLK", pktidx_per_block);
          hputi4(st.buf, "BLOCSIZE", eff_block_size);
        }

        if(curtime != lasttime) {
          lasttime = curtime;
          hputs(st.buf,  "DAQPULSE", timestr);
        }
      }
      hashpipe_status_unlock_safe(&st);

      // TODO Parse the A.B.C.D+N notation
      if((pchar = strchr(dest_ip_str, '+'))) {
        // Null terminate dest_ip_str and point to N
        *pchar = '\0';
        pchar++;
        nstreams = strtoul(pchar, NULL, 0);
        nstreams++;
#ifdef USE_IBVERBS
        if(nstreams > hibv_ctx->max_flows) {
          nstreams = hibv_ctx->max_flows;
        }
#endif // USE_IBVERBS
      } else {
        nstreams = 1;
      }

      // If obs_info is valid and DESTIP is valid and non-zero, start
      // listening!  Valid here just means that it parses OK via inet_aton, not
      // that it is correct and actually usable.
      if(mk_obs_info_valid(obs_info) &&
          inet_aton(dest_ip_str, &dest_ip) &&
          dest_ip.s_addr != INADDR_ANY) {

#ifdef USE_IBVERBS
        // Add flow(s) and change state to listen
        hashpipe_info("hpguppi_meerkat_net_thread", "dest_ip %s+%s flows", dest_ip_str, pchar ? pchar : "X");
        hashpipe_info("hpguppi_meerkat_net_thread", "adding %d flows", nstreams);
        for(dest_idx=0; dest_idx < nstreams; dest_idx++) {
          if(hashpipe_ibv_flow(hibv_ctx, dest_idx, IBV_FLOW_SPEC_UDP,
                hibv_ctx->mac, NULL, 0, 0,
                0, ntohl(dest_ip.s_addr)+dest_idx, 0, net_params->port))
          {
            hashpipe_error(
                "hpguppi_meerkat_net_thread", "hashpipe_ibv_flow error");
            break;
          }
        }
        // Check if all streams/flows were added successfully
        if(dest_idx < nstreams) {
          // TODO Remove flows that were added
          // Stay in IDLE state loop
          continue;
        }
#else
        // If multicast address(es)
        for(dest_idx=0; dest_idx < nstreams; dest_idx++) {
          dest_ip_tmp.s_addr = htonl(ntohl(dest_ip.s_addr)+dest_idx);
          if((IN_MULTICAST(ntohl(dest_ip_tmp.s_addr)))) {
            // IP_ADD_MEMBERSHIP
            if(hpguppi_mcast_membership(mcast_subscriber,
                  net_params->ifname, IP_ADD_MEMBERSHIP, &dest_ip_tmp)) {
              hashpipe_error("hpguppi_meerkat_net_thread",
                  "could not add mcast membership for group %s+%d",
                  dest_ip_str, dest_idx);
              break;
            }
          }
          // Check if all addresses were processed successfully
          if(dest_idx < nstreams) {
            // TODO Drop memberships that were added
            // Stay in IDLE state loop
            continue;
          }
        }
        // Remember multicast group
        // TODO Add nstreams field to remember nstreams?
        net_params->mcast_group.s_addr = dest_ip.s_addr;
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
          if((pchar = strchr(dest_ip_str, '+'))) *pchar = '\0';
        }
        hashpipe_status_unlock_safe(&st);

        // If DESTIP is invalid or zero, go to IDLE state.  Invalid here just
        // means that it fails to parse, not that it is incorrect or otherwise
        // unusable.
        if(!inet_aton(dest_ip_str, &dest_ip) || dest_ip.s_addr == INADDR_ANY) {
#ifdef USE_IBVERBS
          // Remove flow(s) and change state to listen
          hashpipe_info("hpguppi_meerkat_net_thread", "dest_ip %s (removing %d flows)", dest_ip_str, nstreams);
          for(dest_idx=0; dest_idx < nstreams; dest_idx++) {
            if(hashpipe_ibv_flow(hibv_ctx, dest_idx, IBV_FLOW_SPEC_UDP,
                  0, 0, 0, 0, 0, 0, 0, 0))
            {
              hashpipe_error(
                  "hpguppi_meerkat_net_thread", "hashpipe_ibv_flow error");
            }
          }
          nstreams = 0;
#else
          // IP_DROP_MEMBERSHIP
          if(hpguppi_mcast_membership(mcast_subscriber,
                net_params->ifname, IP_DROP_MEMBERSHIP, &net_params->mcast_group)) {
            hashpipe_warn("hpguppi_meerkat_net_thread",
                "could not add mcast membership for group %.8x",
                ntohl(net_params->mcast_group.s_addr));
          }
          // Forget mcast_group
          net_params->mcast_group.s_addr = INADDR_ANY;
#endif // USE_IBVERBS

          // Switch to IDLE state (and ensure waiting flag is clear)
          state = IDLE;
          waiting = 0;
          // Re-init obs_info, pktidx_per_block, and eff_block_size to invalid
          // values
          mk_obs_info_init(&obs_info);
          pktidx_per_block = 0;
          eff_block_size = 0;

          // Update DAQSTATE, status_key, and obs_info params
          hashpipe_status_lock_safe(&st);
          {
            hputs(st.buf, "DAQSTATE", "IDLE");
            hputs(st.buf, status_key, "idle");
            // These must be reset to valid values by external actor
            hputu4(st.buf, "FENCHAN", obs_info.fenchan);
            hputu4(st.buf, "NANTS",   obs_info.nants);
            hputu4(st.buf, "NSTRM",   obs_info.nstrm);
            hputu4(st.buf, "HNTIME",  obs_info.hntime);
            hputu4(st.buf, "HNCHAN",  obs_info.hnchan);
            hputu8(st.buf, "HCLOCKS", obs_info.hclocks);
            hputi4(st.buf, "SCHAN",   obs_info.schan);
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

// IBVERBS can return multiple packets, so we need to loop over all of them.
// PKTSOCK only returns one frame at a time, so no looping needed.
#ifdef USE_IBVERBS
    // For each packet: process all packets
    for(curr_rpkt = hibv_rpkt; curr_rpkt;
        //curr_rpkt = (struct hashpipe_ibv_recv_pkt *)curr_rpkt->wr.next) {
        curr_rpkt = (struct hashpipe_ibv_recv_pkt *)(((struct ibv_recv_wr *)curr_rpkt)->next)) {
      // Get pointer to packet data
      p_udppkt = (struct udppkt *)curr_rpkt->wr.sg_list->addr;
#else
      // Get pointer to ethernet frame
      p_udppkt = (struct udppkt *)(PKT_MAC(p_frame));
#endif // USE_IBVERBS

      // TODO Validate that this is a valid packet for us!
#if 0
for(debug_i=0; debug_i<8; debug_i++) {
  printf("%04x:", 16*debug_i);
  for(debug_j=0; debug_j<16; debug_j++) {
    printf(" %02x", ((uint8_t *)p_udppkt)[16*debug_i+debug_j]);
  }
  printf("\n");
}
printf("\n");
fflush(stdout);
#endif

      // Parse packet
      p_spead_payload = mk_parse_mkfeng_packet(p_udppkt, &feng_spead_info);

      // Ignore packets with FID >= NANTS
      if(feng_spead_info.feng_id >= obs_info.nants) {
#ifndef USE_IBVERBS
        hashpipe_pktsock_release_frame(p_frame);
#endif // USE_IBVERBS
        continue;
      }

      // Count packet
      packet_count++;

      // Get packet index and absolute block number for packet
      pkt_seq_num = mk_pktidx(obs_info, feng_spead_info);
      pkt_blk_num = pkt_seq_num / pktidx_per_block;

#if 0
printf("pkt_seq_num = %lu\n", pkt_seq_num);
printf("pkt_blk_num = %lu\n", pkt_blk_num);
printf("heap_counter = 0x%016lx\n", feng_spead_info.heap_counter);
printf("heap_size    = 0x%016lx\n", feng_spead_info.heap_size   );
printf("heap_offset  = 0x%016lx\n", feng_spead_info.heap_offset );
printf("payload_size = 0x%016lx\n", feng_spead_info.payload_size);
printf("timestamp    = 0x%016lx\n", feng_spead_info.timestamp   );
printf("feng_id      = 0x%016lx\n", feng_spead_info.feng_id     );
printf("feng_chan    = 0x%016lx\n", feng_spead_info.feng_chan   );
printf("\n");
#endif

      // We update the status buffer at the start of each block
      // Also read PKTSTART, DWELL to calculate start/stop seq numbers.
      if(pkt_seq_num % pktidx_per_block == 0
          && pkt_seq_num != status_seq_num) {
        status_seq_num  = pkt_seq_num;
        hashpipe_status_lock_safe(&st);
        {
          hputi8(st.buf, "PKTIDX", pkt_seq_num);
          hputi8(st.buf, "PKTBLK", pkt_blk_num); // TODO do we want/need this?
          hputi4(st.buf, "BLOCSIZE", eff_block_size);
          hgetu8(st.buf, "PKTSTART", &start_seq_num);
          start_seq_num -= start_seq_num % pktidx_per_block;
          hputu8(st.buf, "PKTSTART", start_seq_num);
          hgetr8(st.buf, "DWELL", &dwell_seconds);
          hputr8(st.buf, "DWELL", dwell_seconds); // In case it wasn't there

          // Get CHAN_BW and calculate/store TBIN
          hgetr8(st.buf, "CHAN_BW", &chan_bw);
          // If CHAN_BW is zero, set to default value (1 MHz)
          if(chan_bw == 0.0) {
            chan_bw = 1.0;
          }
          tbin = 1e-6 / fabs(chan_bw);
          hputr8(st.buf, "TBIN", tbin);

          // Dwell blocks is equal to:
          //
          //       dwell_seconds
          //     ------------------
          //     tbin * ntime/block
          //
          // To get an integer number of blocks, simply truncate
          dwell_blocks = trunc(dwell_seconds / (tbin * mk_ntime(BLOCK_DATA_SIZE, obs_info)));

          stop_seq_num = start_seq_num + pktidx_per_block * dwell_blocks;
          hputi8(st.buf, "PKTSTOP", stop_seq_num);

          hgetu8(st.buf, "NDROP", &u64tmp);
          u64tmp += ndrop_total; ndrop_total = 0;
          hputu8(st.buf, "NDROP", u64tmp);

          hgetu8(st.buf, "NLATE", &u64tmp);
          u64tmp += nlate; nlate = 0;
          hputu8(st.buf, "NLATE", u64tmp);

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

#if 0
          // Calculate receive speed in packets per second
          hputi8(st.buf, "RECVNS", elapsed_recv);
          hputi8(st.buf, "RECVPKTS", count_recv);
          hputr4(st.buf, "RECVPPS", count_recv / (elapsed_recv/1e9));
          elapsed_recv = 0;
          count_recv = 0;
#endif

          // Calculate processing speed in packets per second
          hputi8(st.buf, "PROCNS", elapsed_proc);
          hputi8(st.buf, "PROCPKTS", count_proc);
          hputr4(st.buf, "PROCPPS", count_proc / (elapsed_proc/1e9));
          elapsed_proc = 0;
          count_proc = 0;
        }
        hashpipe_status_unlock_safe(&st);
      } // End status buffer block update

      // Manage blocks based on pkt_blk_num
      if(pkt_blk_num == wblk[1].block_num + 1) {
#if 0
printf("next block (%ld == %ld + 1)\n", pkt_blk_num, wblk[1].block_num);
#endif
        // Finalize first working block
        finalize_block(wblk);
        // Update ndrop counter
        ndrop_total += wblk->ndrop;
        // Shift working blocks
        wblk[0] = wblk[1];
        // Check start/stop using wblk[0]'s first PKTIDX
        state = check_start_stop(&st, wblk[0].block_num * pktidx_per_block);
        // Increment last working block
        increment_block(&wblk[1], pkt_blk_num);
        // Wait for new databuf data block to be free
        wait_for_block_free(&wblk[1], &st, status_key);
      }
      // Check for PKTIDX discontinuity
      else if(pkt_blk_num < wblk[0].block_num - 1
      || pkt_blk_num > wblk[1].block_num + 1) {
#if 0
printf("reset blocks (%ld <> [%ld - 1, %ld + 1])\n", pkt_blk_num, wblk[0].block_num, wblk[1].block_num);
#endif
        // Should only happen when transitioning into LISTEN, so warn about it
        hashpipe_warn("hpguppi_meerkat_net_thread",
            "working blocks reinit due to packet discontinuity (PKTIDX %lu)",
            pkt_seq_num);
        // Re-init working blocks for block number *after* current packet's block
        // and clear their data buffers
        for(wblk_idx=0; wblk_idx<2; wblk_idx++) {
          init_block_info(wblk+wblk_idx, NULL, -1, pkt_blk_num+wblk_idx+1,
              eff_block_size / feng_spead_info.payload_size);
#if 0
          // Clear data buffer
          // TODO Move this out of net thread (takes too long)
          //memset(block_info_data(wblk+wblk_idx), 0, eff_block_size);
          clear_memory(block_info_data(wblk+wblk_idx), eff_block_size);
#endif
        }

        // Check start/stop using wblk[0]'s first PKTIDX
        state = check_start_stop(&st, wblk[0].block_num * pktidx_per_block);
// This happens after discontinuities (e.g. on startup), so don't warn about
// it.
      } else if(pkt_blk_num == wblk[0].block_num - 1) {
        // Ignore late packet, continue on to next one
        // TODO Move this check above the "once per block" status buffer
        // update (so we don't accidentally update status buffer based on a
        // late packet)?
        nlate++;
#if 0
        // Should "never" happen, so warn anbout it
        hashpipe_warn("hpguppi_meerkat_net_thread",
            "ignoring late packet (PKTIDX %lu)",
            pkt_seq_num);
#endif
      }

#if 0
printf("packet block: %ld   working blocks: %ld %lu\n", pkt_blk_num, wblk[0].block_num, wblk[1].block_num);
#endif

      // TODO Check START/STOP status???

      // Once we get here, compute the index of the working block corresponding
      // to this packet.  The computed index may not correspond to a valid
      // working block!
      wblk_idx = pkt_blk_num - wblk[0].block_num;

      // Only copy packet data and count packet if its wblk_idx is valid
      if(0 <= wblk_idx && wblk_idx < 2) {
        // Update block's packets per block.  Not needed for each packet, but
        // probably just as fast to do it for each packet rather than
        // check-and-update-only-if-needed for each packet.
        wblk[wblk_idx].pkts_per_block = eff_block_size / feng_spead_info.payload_size;
        wblk[wblk_idx].pktidx_per_block = pktidx_per_block;

        // Copy packet data to data buffer of working block
        copy_packet_data_to_databuf(wblk+wblk_idx,
            &obs_info, &feng_spead_info, p_spead_payload);

        // Count packet for block and for processing stats
        wblk[wblk_idx].npacket++;
        count_proc++;
      }

#ifdef USE_IBVERBS
    } // end for each packet
#endif // USE_IBVERBS

    clock_gettime(CLOCK_MONOTONIC_RAW, &ts_stop_proc);
    elapsed_proc += ELAPSED_NS(ts_start_proc, ts_stop_proc);

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

static hashpipe_thread_desc_t hpmkat_thread_desc = {
    name: "hpguppi_meerkat_net_thread",
    skey: "NETSTAT",
    init: init,
    run:  run,
    ibuf_desc: {NULL},
    obuf_desc: {hpguppi_input_databuf_create}
};

static __attribute__((constructor)) void ctor()
{
  register_hashpipe_thread(&hpmkat_thread_desc);
}

// vi: set ts=2 sw=2 et :
