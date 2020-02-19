// hpguppi_ibverbs_pkt_thread.c
//
// A Hashpipe thread that receives packets from a network interface using IB
// Verbs.  This is purely a packet capture thread.  Packets are stored in
// blocks in the input data buffer in the orer they are received.  A downstream
// thread can be used to process the packets further.

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
#include <sys/mman.h>
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

#include "hashpipe_ibverbs.h"
#define DEFAULT_SEND_PKT_NUM (1)
#define DEFAULT_RECV_PKT_NUM (32768) // Requires ConnectX-4 or newer
#define DEFAULT_MAX_FLOWS (16)
#define DEFAULT_NUM_QP (1)

#if 0
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
#endif // 0

#define ELAPSED_NS(start,stop) \
  (((int64_t)stop.tv_sec-start.tv_sec)*1000*1000*1000+(stop.tv_nsec-start.tv_nsec))

// TODO Re-wrte this once I figure out how this thread will work...
// Define run states.  Currently two run states are defined: IDLE and CAPTURE.
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
// than 0.0.0.0, the state transitions to the CAPTURE state.
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
#endif

// Wait for a block_info's databuf block to be free, then copy status buffer to
// block's header and clear block's data.  Calling thread will exit on error
// (should "never" happen).  Status buffer updates made after the copy to the
// block's header will not be seen in the block's header (e.g. by downstream
// threads).  Any status buffer fields that need to be updated for correct
// downstream processing of this block must be updated BEFORE calling this
// function.  Note that some of the block's header fields will be set when the
// block is finalized (see finalize_block() for details).
static void wait_for_block_free(hpguppi_input_databuf_t *db, int block_idx,
    hashpipe_status_t * st, const char * status_key)
{
  int rv;
  char pktstat[80];
  char pktbuf_status[80];
  int pktbuf_full = hpguppi_input_databuf_total_status(db);
  sprintf(pktbuf_status, "%d/%d", pktbuf_full, db->header.n_block);
  //TODO pktstruct timespec ts_sleep = {0, 10 * 1000 * 1000}; // 10 ms

  hashpipe_status_lock_safe(st);
  {
    // Save original status
    hgets(st->buf, status_key, sizeof(pktstat), pktstat);
    // Set "waitfree" status
    hputs(st->buf, status_key, "waitfree");
    // Update PKTBUFST
    hputs(st->buf, "PKTBUFST", pktbuf_status);
  }
  hashpipe_status_unlock_safe(st);

  while ((rv=hpguppi_input_databuf_wait_free(db, block_idx))
      != HASHPIPE_OK) {
    if (rv==HASHPIPE_TIMEOUT) {
      pktbuf_full = hpguppi_input_databuf_total_status(db);
      sprintf(pktbuf_status, "%d/%d", pktbuf_full, db->header.n_block);
      hashpipe_status_lock_safe(st);
      {
        hputs(st->buf, status_key, "blocked");
        hputs(st->buf, "PKTBUFST", pktbuf_status);
      }
      hashpipe_status_unlock_safe(st);
    } else {
      hashpipe_error(__FUNCTION__,
          "error waiting for free databuf (%s)", __FILE__);
      pthread_exit(NULL);
    }
  }
  hashpipe_status_lock_safe(st);
  {
    // Restore original status
    hputs(st->buf, status_key, pktstat);
  }
  hashpipe_status_unlock_safe(st);

#if 0
  // TODO Move this out of net thread (takes too long)
  // TODO Just clear effective block size?
  //memset(block_info_data(bi), 0, BLOCK_DATA_SIZE);
  clear_memory(block_info_data(bi), BLOCK_DATA_SIZE);
#else
  //TODO nanosleep(&ts_sleep, NULL);
#endif
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
// - port is the UDP port which we will listen to.
// - hibv_ctx is the hashpipe_ibv_context structure that Hashpipe uses to
//   manage the packet socket connection.
struct net_params {
  struct hashpipe_ibv_context hibv_ctx;
  int port;
};

// This thread's init() function, if provided, is called by the Hashpipe
// framework at startup to allow the thread to perform initialization tasks
// such as setting up network connections or GPU devices.
static int init(hashpipe_thread_args_t *args)
{
  // Local aliases to shorten access to args fields
  hashpipe_status_t st = args->st;
  const char * thread_name = args->thread_desc->name;
  const char * status_key = args->thread_desc->skey;

  int i;
  char dest_ip[80];
  struct net_params * net_params;
  struct hashpipe_ibv_context * hibv_ctx;

  // Allocate (and clear) net_params structure
  net_params = (struct net_params *)calloc(1, sizeof(struct net_params));
  if(!net_params) {
    return HASHPIPE_ERR_SYS;
  }
  hibv_ctx = &net_params->hibv_ctx;

  // Set defaults (some of which can't be overridden, e.g. pkt_size_max)
  // Initialize the hibv_ctx fields that we need for buffer allocation in this
  // function.
  strcpy(hibv_ctx->interface_name, "eth4");
  net_params->port = 7148;
  strcpy(dest_ip, "0.0.0.0");
  hibv_ctx->pkt_size_max = MAX_PKT_SIZE;
  hibv_ctx->max_flows = DEFAULT_MAX_FLOWS;
  hibv_ctx->nqp = DEFAULT_NUM_QP;

  hashpipe_status_lock_safe(&st);
  {
    // Get info from status buffer if present (no change if not present)
    hgets(st.buf,  "BINDHOST",
        sizeof(hibv_ctx->interface_name), hibv_ctx->interface_name);
    hgeti4(st.buf, "BINDPORT", &net_params->port);
    hgetu4(st.buf, "MAXFLOWS", &hibv_ctx->max_flows);

    // Sanity checks
    if(hibv_ctx->max_flows == 0) {
      hibv_ctx->max_flows = 1;
    }

    // Store bind host/port info etc in status buffer (in case it was not there
    // before).
    hputs(st.buf, "BINDHOST", hibv_ctx->interface_name);
    hputi4(st.buf, "BINDPORT", net_params->port);
    hputs(st.buf, "DESTIP", dest_ip);
    hputu4(st.buf, "MAXFLOWS", hibv_ctx->max_flows);

    hputu8(st.buf, "PKTPKTS", 0);
    // PKTBLKIN is the absolute block number of the next block to be marked
    // filled.
    hputu8(st.buf, "PKTBLKIN", 0);
    // Set status_key to init
    hputs(st.buf, status_key, "init");
  }
  hashpipe_status_unlock_safe(&st);

  // Calculate total number of receive packets per block.  This is the number
  // of hashpipe_recv_pkt and ibv_sge elements that we need to allocate.
  //uint32_t total_recv_pkts = BLOCK_DATA_SIZE / hibv_ctx->pkt_size_max;
  uint32_t total_recv_pkts = 32768;

  // hashpipe_ibv_init() needs to know the number of recv_pkts per WQ.  It is
  // considered an error if the number of WQs does not evenly divide the number
  // of receive packets.
  if(total_recv_pkts % hibv_ctx->nqp != 0) {
    // Log error and return error code
    hashpipe_error(thread_name,
        "BLOCK_DATA_SIZE / pkt_size %% nqp != 0 (%lu / %u %% %u != 0)",
        BLOCK_DATA_SIZE, hibv_ctx->pkt_size_max, hibv_ctx->nqp);
    return HASHPIPE_ERR_PARAM;
  }
  hibv_ctx->recv_pkt_num = total_recv_pkts / hibv_ctx->nqp;

  // We are RX only, but we still need to provide one hashpipe_send_pkt and
  // ibv_sge element for sending per work queue.
  hibv_ctx->send_pkt_num = 1;
  uint32_t total_send_pkts = hibv_ctx->send_pkt_num * hibv_ctx->nqp;

  // We are going to manage the buffers ourselves!
  hibv_ctx->user_managed_flag = 1;

  // Allocate packet buffers
  if(!(hibv_ctx->send_pkt_buf = (struct hashpipe_ibv_send_pkt *)calloc(
      total_send_pkts, sizeof(struct hashpipe_ibv_send_pkt)))) {
    return HASHPIPE_ERR_SYS;
  }
  if(!(hibv_ctx->recv_pkt_buf = (struct hashpipe_ibv_recv_pkt *)calloc(
      total_recv_pkts, sizeof(struct hashpipe_ibv_recv_pkt)))) {
    return HASHPIPE_ERR_SYS;
  }

  // Allocate sge buffers.  We actually allocate 3 SGEs per recevie WR so that
  // we can control/optimize the memory alignment of different parts of the
  // packet: Ethernet/IP/UDP headers, SPEAD headers, and SPEAD payload.  The
  // packets are structured like this:
  //
  //     0x0000:  0100 5e09 02a8 248a 07cc 7e48 0800 4500
  //     0x0010:  047c 0000 4000 fc11 7652 0a64 0809 ef09
  //     0x0020:  02a8 1bec 1bec 0468 0000 5304 0206 0000
  //     0x0030:  000b 8001 6068 752a 8032 8002 0000 0000
  //     0x0040:  4000 8003 0000 0000 3c00 8004 0000 0000
  //     0x0050:  0400 9600 6068 7520 0000 c101 0000 0000
  //     0x0060:  0032 c103 0000 0000 0a80 4300 0000 0000
  //     0x0070:  0000 8000 0000 0000 0000 8000 0000 0000
  //     0x0080:  0000 8000 0000 0000 0000 fdfc 08f9 0a03
  //     0x0090:  05fe 0509 0404 fdfa 06f5 0605 01fb 03fe
  //     0x00a0:  fcfe 01fd ff02 fdff 02fb 000b fd09 05fc
  //
  // By using 3 SGEs, we can arrange this in memory like this:
  //
  //     0x0000:  0100 5e09 02a8 248a 07cc 7e48 0800 4500
  //     0x0010:  047c 0000 4000 fc11 7652 0a64 0809 ef09
  //     0x0020:  02a8 1bec 1bec 0468 0000 ---- ---- ----
  //     0x0030:  ---- ---- ---- ---- ---- ---- ---- ----
  //     0x0040:  5304 0206 0000 000b 8001 6068 752a 8032
  //     0x0050:  8002 0000 0000 4000 8003 0000 0000 3c00
  //     0x0060:  8004 0000 0000 0400 9600 6068 7520 0000
  //     0x0070:  c101 0000 0000 0032 c103 0000 0000 0a80
  //     0x0080:  4300 0000 0000 0000 8000 0000 0000 0000
  //     0x0090:  8000 0000 0000 0000 8000 0000 0000 0000
  //     0x00a0:  ---- ---- ---- ---- ---- ---- ---- ----
  //     0x00b0:  ---- ---- ---- ---- ---- ---- ---- ----
  //     0x00c0:  fdfc 08f9 0a03 05fe 0509 0404 fdfa 06f5
  //     0x00d0:  0605 01fb 03fe fcfe 01fd ff02 fdff 02fb
  //
  // This alignment "wastes" 54 bytes of memory (shown as "--" above), but the
  // packet buffer is oversized by more than that many bytes, so really we are
  // just redistributing the "wasted" bytes.

  if(!(hibv_ctx->send_sge_buf = (struct ibv_sge *)calloc(
      total_send_pkts, sizeof(struct ibv_sge)))) {
    return HASHPIPE_ERR_SYS;
  }
  if(!(hibv_ctx->recv_sge_buf = (struct ibv_sge *)calloc(
      3*total_recv_pkts, sizeof(struct ibv_sge)))) {
    return HASHPIPE_ERR_SYS;
  }

  // send_mr_size and send_mr_buf are set here.
  // recv_mr_size and recv_mr_buf are set in run().
  hibv_ctx->send_mr_size = (size_t)total_send_pkts * hibv_ctx->pkt_size_max;

  // Allocate memory for send_mr_buf
  if(!(hibv_ctx->send_mr_buf = (uint8_t *)calloc(
      total_send_pkts, hibv_ctx->pkt_size_max))) {
    return HASHPIPE_ERR_SYS;
  }

  // Initialize the `num_sge` field of the send WRs and the `addr`, `length`
  // fields of the the send SGEs.  The library will initialize the rest of the
  // send WR fields and send SGEs.  The receive WRs and SGEs will be
  // initialized in run() after the data blocks have been "acquired" (i.e.
  // waited until free).
  for(i=0; i<total_send_pkts; i++) {
    hibv_ctx->send_pkt_buf[i].wr.num_sge = 1;

    hibv_ctx->send_sge_buf[i].addr = (uint64_t)
      hibv_ctx->send_mr_buf + i * hibv_ctx->pkt_size_max;
    hibv_ctx->send_sge_buf[i].length = hibv_ctx->pkt_size_max;
  }

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
  const char * thread_name = args->thread_desc->name;
  const char * status_key = args->thread_desc->skey;
  struct net_params *net_params = (struct net_params *)args->user_data;
  struct hashpipe_ibv_context * hibv_ctx = &net_params->hibv_ctx;

  // Misc counters, etc
  uintptr_t rv;
  int i, ii, j;
  uint64_t wr_id;
  uint64_t base_addr;
  // String version of destination address
  char dest_ip_stream_str[80] = {};
  char dest_ip_stream_str_new[80] = {};
  char * pchar;
  // Numeric form of dest_ip
  struct in_addr dest_ip;
  int dest_idx;
  // Number of destination IPs we are listening for
  int nstreams = 0;

  char pktbuf_status[80];
  int pktbuf_full;

  // databuf_end is used to test when a pointer has advanced beyond the end of
  // the databuf blocks'
  uintptr_t databuf_end = ((uintptr_t)db->block) + sizeof(db->block);

  // We maintain three active blocks at all times.  curblk is the number of the
  // oldest of the three blocks with block numbers curblk+1 and curblk+2 being
  // the other two of the three active blocks.  Work requests are first posted
  // with locations in curblk as the destination.  Enough WRs are posted to
  // cover the entire block.  When packets are received, their WRs' SGEs are
  // updated to point to one block higher (with wrapping) than they currently
  // point.  If any of these SGEs point to block that is greater than curblk+2,
  // then block curblk is marked filled and curblk is incremented.  This
  // continues indefinitely.  We use three blocks because using two blocks is
  // not enough.  It is possible that one QP will return a group of work
  // requests that include the last packet of curblk and the first packet of
  // curblk+1.  If we used only two blocks, this would result in the marking
  // filled of curblk even though other QPs may have not yet returned their
  // final packet(s) for curblk.
  uint64_t curblk = 0;

  // Used to track current sub-block number of each work request.  Each block
  // is 128 MB, which can hold 64K packets that are (up to) 2KB in size.  We
  // can only post 32K work requests at a time so we divide each block into
  // sub-blocks such that each sub-block holds 32K packets.  Each WR then
  // corresponds to the same "slot" in each sub-block.
  uint64_t * pkt_blocks;
  
  if(!(pkt_blocks = (uint64_t *)
        calloc(hibv_ctx->recv_pkt_num*hibv_ctx->nqp, sizeof(uint64_t)))) {
    hashpipe_error(thread_name, "cannot allocate pkt_blocks array");
    return (void *)HASHPIPE_ERR_SYS;
  }

  // Wait until the first three blocks are marked as free
  // (should already be free)
  for(i=0; i<3; i++) {
    wait_for_block_free(db, (curblk+i) % N_INPUT_BLOCKS, &st, status_key);
  }

  // send_mr_size and send_mr_buf are set in init().
  // recv_mr_size and recv_mr_buf are set here.  NB: This is necessary because
  // shared memory mappings change between init() and run()!
  hibv_ctx->recv_mr_size = sizeof(db->block);
  hibv_ctx->recv_mr_buf = (uint8_t *)db->block;

  hashpipe_info(thread_name, "Setting up recv_mr start %p length %lu",
      hibv_ctx->recv_mr_buf, hibv_ctx->recv_mr_size);

  // The hashpipe_ibv_init() function will link the hashpipe_ibv_recv_pkt
  // (WR) elements of the recv_pkt_buf into `nqp` linked lists, with
  // `recv_pkt_num` work requests per list.  It will also set the `wr_id` field
  // of the WRs and point their `sg_list` fields to the correponding SGE
  // elements.
  //
  // We need to set the `num_sge` field of the work requests (to 1) and set the
  // `addr` and `length` fields of the SGEs.  We interleave the SGE destination
  // buffers so that each work queue's packets will be spread evenly throughout
  // the block.  We start with the first block (i.e. "curblk")
  for(i=0; i<hibv_ctx->nqp; i++) {
    // First index of this QP's recv_{pkt,sge}_buf
    ii = i * hibv_ctx->recv_pkt_num;

    for(j=0; j<hibv_ctx->recv_pkt_num; j++) {
      // Technically redundant since curblk is zero and pkt_blocks is
      // initialized to zeros.
      pkt_blocks[ii+j] = curblk;
      hibv_ctx->recv_pkt_buf[ii+j].wr.num_sge = 3;

      base_addr = (uint64_t)(db->block[curblk].data +
          hibv_ctx->pkt_size_max * (ii+j));
      hibv_ctx->recv_sge_buf[3*(ii+j)  ].addr = base_addr;
      hibv_ctx->recv_sge_buf[3*(ii+j)+1].addr = base_addr + 0x40;
      hibv_ctx->recv_sge_buf[3*(ii+j)+2].addr = base_addr + 0xc0;
      hibv_ctx->recv_sge_buf[3*(ii+j)  ].length = 42;
      hibv_ctx->recv_sge_buf[3*(ii+j)+1].length = 96;
      hibv_ctx->recv_sge_buf[3*(ii+j)+2].length = hibv_ctx->pkt_size_max - 0xc0;

      if(hibv_ctx->recv_sge_buf[ii+j].addr < (uintptr_t)db->block
      || hibv_ctx->recv_sge_buf[ii+j].addr > databuf_end) {
        hashpipe_error(thread_name,
          "bad pointer math i=%d j=%d ii=%d addr=%p db_start=%p db_end=%p",
          i, j, ii, hibv_ctx->recv_sge_buf[ii+j].addr,
          db->block[curblk].data, databuf_end);
      }
    }
  }

  // Diagnostic info
  for(i=0; i<hibv_ctx->nqp; i++) {
    // First index of this QP's recv_{pkt,sge}_buf
    ii = i * hibv_ctx->recv_pkt_num;

    j = 0;
    hashpipe_info(thread_name,
      "i=%d j=%d ii=%d addr=%p db_start=%p db_end=%p pktblk %d curblk %d",
      i, j, ii, hibv_ctx->recv_sge_buf[ii+j].addr,
      db->block, databuf_end, pkt_blocks[ii+j], curblk);

    j = hibv_ctx->recv_pkt_num-1;
    hashpipe_info(thread_name,
      "i=%d j=%d ii=%d addr=%p db_start=%p db_end=%p",
      i, j, ii, hibv_ctx->recv_sge_buf[ii+j].addr,
      db->block, databuf_end, pkt_blocks[ii+j], curblk);
  }

  // Initialize ibverbs
  if((rv = hashpipe_ibv_init(hibv_ctx))) {
    hashpipe_error(thread_name, "Error initializing ibverbs.");
    return (void *)rv;
  }

  hashpipe_info(thread_name, "recv_pkt_num=%u max_flows=%u num_qp=%u",
      hibv_ctx->recv_pkt_num, hibv_ctx->max_flows, hibv_ctx->nqp);

#if 0
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

  char timestr[32] = {0};
#endif

  // Heartbeat variables
  time_t lasttime = 0;
  time_t curtime = 0;

  // Variables for counting packets and bytes.
  uint64_t packet_count = 0; // Counts packets (TODO between updates to status buffer?)
  //uint64_t u64tmp = 0; // Used for status buffer interactions
  //uint64_t max_recvpkt_count = 0;
#if 0
  uint64_t ndrop_total = 0;
  uint64_t nlate = 0;
#endif

  // Variables for handing received packets
  struct hashpipe_ibv_recv_pkt * hibv_rpkt = NULL;
  struct hashpipe_ibv_recv_pkt * curr_rpkt;

#if 0
  // Variables for tracking timing stats
  //
  // ts_start_recv(N) to ts_stop_recv(N) is the time spent in the "receive" call.
  // ts_stop_recv(N) to ts_start_recv(N+1) is the time spent processing received data.
  struct timespec ts_start_recv = {0}, ts_stop_recv = {0};
  struct timespec ts_prev_phys = {0}, ts_curr_phys = {0};

  // We compute NETGBPS every block as (bits_processed_net / ns_processed_net)
  // We compute NETPKPS every block as (1e9 * pkts_processed_net / ns_processed_net)
  float netgbps = 0.0, netpkps = 0.0;
  uint64_t bits_processed_net = 0;
  uint64_t pkts_processed_net = 0;
  uint64_t ns_processed_net = 0;

  // We compute PHYSGBPS every second as (bits_processed_phys / ns_processed_phys)
  // We compute PHYSPKPS every second as (1e9 * pkts_processed_phys / ns_processed_phys)
  float physgbps = 0.0, physpkps = 0.0;
  uint64_t bits_processed_phys = 0;
  uint64_t pkts_processed_phys = 0;
  uint64_t ns_processed_phys = 0;
#endif

  // Get DESTIP and update status_key with running state
  hashpipe_status_lock_safe(&st);
  {
    hgets(st.buf,  "DESTIP", sizeof(dest_ip_stream_str), dest_ip_stream_str);
    hputs(st.buf, status_key, "running");
  }
  hashpipe_status_unlock_safe(&st);

  // Main loop
  while (run_threads()) {

    hibv_rpkt = hashpipe_ibv_recv_pkts(hibv_ctx, 50); // 50 ms timeout

    time(&curtime);

    if(!hibv_rpkt) {
      if(errno) {
        perror("hashpipe_ibv_recv_pkts");
        errno = 0;
      }
      // Timeout?
      if(curtime == lasttime) {
        // No, continue receiving
        continue;
      }
    }

    // Got packets or timeout (with or without error)

    // We perform some status buffer updates every second
    if(lasttime != curtime) {
      lasttime = curtime;

      // Update pktbuf status
      pktbuf_full = hpguppi_input_databuf_total_status(db);
      sprintf(pktbuf_status, "%d/%d", pktbuf_full, db->header.n_block);

      // Update status buffer fields, get dest_ip
      hashpipe_status_lock_safe(&st);
      {
        hputu8(st.buf, "PKTBLKIN", curblk);
        hputs(st.buf, "PKTBUFST", pktbuf_status);
        hputu8(st.buf, "PKTPKTS", packet_count);

        // Get DESTIP address
        hgets(st.buf,  "DESTIP",
            sizeof(dest_ip_stream_str_new), dest_ip_stream_str_new);
      }
      hashpipe_status_unlock_safe(&st);

      // If DESTIP has changed
      if(strcmp(dest_ip_stream_str, dest_ip_stream_str_new)) {

        // Make sure the change is allowed
        // If we are listening, the only allowed change is to "0.0.0.0"
        if(nstreams > 0 && strcmp(dest_ip_stream_str_new, "0.0.0.0")) {
          hashpipe_error(thread_name,
              "already listening to %s, can't switch to %s",
              dest_ip_stream_str, dest_ip_stream_str_new);
        } else {
          // Parse the A.B.C.D+N notation
          //
          // Nul terminate at '+', if present
          if((pchar = strchr(dest_ip_stream_str_new, '+'))) {
            // Null terminate dest_ip portion and point to N
            *pchar = '\0';
          }

          // If the IP address fails to satisfy aton()
          if(!inet_aton(dest_ip_stream_str_new, &dest_ip)) {
            hashpipe_error(thread_name, "invalid DESTIP: %s", dest_ip_stream_str_new);
          } else {
            // If switching to "0.0.0.0"
            if(dest_ip.s_addr == INADDR_ANY) {
              // Remove all flows
              hashpipe_info(thread_name, "dest_ip %s (removing %d flows)",
                  dest_ip_stream_str_new, nstreams);
              for(dest_idx=0; dest_idx < nstreams; dest_idx++) {
                if(hashpipe_ibv_flow(hibv_ctx, dest_idx, IBV_FLOW_SPEC_UDP,
                      0, 0, 0, 0, 0, 0, 0, 0))
                {
                  hashpipe_error(thread_name, "hashpipe_ibv_flow error");
                }
              }
              nstreams = 0;
              // TODO Update the IDLE/CAPTURE state???
            } else {
              // Get number of streams
              nstreams = 1;
              if(pchar) {
                nstreams = strtoul(pchar+1, NULL, 0);
                nstreams++;
              }
              if(nstreams > hibv_ctx->max_flows) {
                nstreams = hibv_ctx->max_flows;
              }
              // Add flows for stream
              hashpipe_info(thread_name, "dest_ip %s+%s flows",
                  dest_ip_stream_str_new, pchar ? pchar+1 : "0");
              hashpipe_info(thread_name, "adding %d flows", nstreams);
              for(dest_idx=0; dest_idx < nstreams; dest_idx++) {
                if(hashpipe_ibv_flow(hibv_ctx, dest_idx, IBV_FLOW_SPEC_UDP,
                      hibv_ctx->mac, NULL, 0, 0,
                      0, ntohl(dest_ip.s_addr)+dest_idx, 0, net_params->port))
                {
                  hashpipe_error(thread_name, "hashpipe_ibv_flow error");
                  break;
                }
              }
              // TODO Update the IDLE/CAPTURE state???
            } // end zero/non-zero IP

            // Restore '+' if it was found
            if(pchar) {
              *pchar = '+';
            }
            // Save the new DESTIP string
            strncpy(dest_ip_stream_str, dest_ip_stream_str_new,
                sizeof(dest_ip_stream_str));
          } // end ip valid
        } // end destip change allowed

        // Store (possibly unchanged) DESTIP/NSTRM
        hashpipe_status_lock_safe(&st);
        {
          hputs(st.buf,  "DESTIP", dest_ip_stream_str);
          hputu4(st.buf, "NSTRM", nstreams);
        }
        hashpipe_status_unlock_safe(&st);
      } // end destip changed
    } // end 1 second update

    // For each packet: update SGE addr
    for(curr_rpkt = hibv_rpkt; curr_rpkt;
        curr_rpkt = (struct hashpipe_ibv_recv_pkt *)curr_rpkt->wr.next) {

      if(curr_rpkt->length == 0) {
        hashpipe_error(thread_name,
            "WR %d got error for block %d when using address: %p (databuf %p %p)",
            curr_rpkt->wr.wr_id, pkt_blocks[curr_rpkt->wr.wr_id],
            curr_rpkt->wr.sg_list->addr,
            db->block, databuf_end);
        pthread_exit(NULL);
      }
#if 0
      else {
        hashpipe_error(thread_name,
            "pktcnt %lu curblk %d WR %d block %d address: %p (databuf %p %p)",
            packet_count, curblk,
            curr_rpkt->wr.wr_id, pkt_blocks[curr_rpkt->wr.wr_id],
            curr_rpkt->wr.sg_list->addr,
            db->block, databuf_end);
      }
#endif

      // Count packet
      packet_count++;

#if 1
      wr_id = curr_rpkt->wr.wr_id;

      // Increment current packet's sub-block
      pkt_blocks[wr_id]++;

      // Set current pkt's new destination addresses for all SGEs
      base_addr = (uint64_t)(
        ((uint8_t *)db->block[(pkt_blocks[wr_id]/2) % N_INPUT_BLOCKS].data) +
        wr_id * hibv_ctx->pkt_size_max +
        (BLOCK_DATA_SIZE/2)*(pkt_blocks[wr_id]%2));
      curr_rpkt->wr.sg_list[0].addr = base_addr;
      curr_rpkt->wr.sg_list[1].addr = base_addr + 0x40;
      curr_rpkt->wr.sg_list[2].addr = base_addr + 0xc0;

      // Sanity check
      if(curr_rpkt->wr.sg_list->addr < (uintptr_t)db->block
      || curr_rpkt->wr.sg_list->addr > databuf_end) {
        hashpipe_error(thread_name,
          "bad pointer math blk=%d (%d) wr_id=%d size=%d addr=%p db_start=%p db_end=%p",
          pkt_blocks[wr_id],
          pkt_blocks[wr_id] % N_INPUT_BLOCKS,
          wr_id, curr_rpkt->wr.sg_list->addr,
          db->block[curblk].data, databuf_end);
        pthread_exit(NULL);
      }

      // If time to advance the ring buffer blocks
      if(pkt_blocks[wr_id] > 2*(curblk+2)) {
        // Mark curblk as filled
        hpguppi_input_databuf_set_filled(db, curblk % N_INPUT_BLOCKS);
        
        // Increment curblk
        curblk++;

        // Wait for curblk+2 to be free
        wait_for_block_free(db, (curblk+2) % N_INPUT_BLOCKS, &st, status_key);

        // Update PKTBLKIN/PKTPKTS in status buffer
        hashpipe_status_lock_safe(&st);
        {
          hputu8(st.buf, "PKTBLKIN", curblk);

#if 0
          hgetu8(st.buf, "PKTPKTS", &u64tmp);
          u64tmp += packet_count; packet_count = 0;
          hputu8(st.buf, "PKTPKTS", u64tmp);
#endif
          hputu8(st.buf, "PKTPKTS", packet_count);
        }
        hashpipe_status_unlock_safe(&st);
      } // end block advance
#endif
    } // end for each packet

    // Release packets (i.e. repost work requests)
    if(hashpipe_ibv_release_pkts(hibv_ctx,
          (struct hashpipe_ibv_recv_pkt *)hibv_rpkt)) {
      perror("hashpipe_ibv_release_pkts");
    }

    // Will exit if thread has been cancelled
    pthread_testcancel();
  } // end main loop

  // Update status_key with exiting state
  hashpipe_status_lock_safe(&st);
  {
    hputs(st.buf, status_key, "exiting");
  }
  hashpipe_status_unlock_safe(&st);

  pthread_exit(NULL);

  return NULL;
}

static hashpipe_thread_desc_t hpmkat_thread_desc = {
    name: "hpguppi_meerkat_pkt_thread",
    skey: "PKTSTAT",
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
