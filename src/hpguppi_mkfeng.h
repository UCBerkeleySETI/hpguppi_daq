// # hpguppi_mkfeng.h - Definitions for MeerKAT F-Engine packets
//
// ## MeerKAT F-Engine packets
//
// The MeerKAT F-Engines send out channelized antenna voltages over a number of
// UDP multicast groups.  Within the MeerKAT vernacular, these multicast groups
// are also referred to as "streams".  The F-Engines operate in one of several
// modes, defined by the number of channels that they produce: 1K mode (1024
// channels), 4K mode (4096 channels), and 32K mode (32768 channels).  The
// number of channels of the current F-Engine mode will be stored in the status
// buffer by an external actor (e.g. the command line or the coordinator) under
// the key "FENCHAN".  For a given mode and a given number of antennas, the
// F-Engines will utilize some number of streams.  The coordinator will learn
// the total number of F-Engine streams and store it in the status buffer under
// the key "FENSTRM".  Likewise, the coordinator will learn the number of
// antennas in the current subarray and will store that value in the status
// buffer under the key "NANTS".
//
// The packets sent by the F-Engines are SPEAD packets, which can be assembled
// into SPEAD heaps.  Each packet specifies the size of the heap to which it
// belongs, but for all(?) current MeerKAT F-Engines the heap size is 256 KiB.
//
// Each heap contains HNTIME samples of HNCHAN frequency channels from a single
// F engine.  The coordinator will populate these value in the status buffer.
// The samples in a heap are arranged in "time major" order, i.e. HNTIME
// samples for a given frequency channel followed by HNTIME samples for the
// next frequency channel and so on for all the channels of the heap.
//
// The number of channels per heap, HNCHAN, is related to the number of F
// engine channels and streams:
//
//               FENCHAN
//     HNCHAN = ---------
//               FENSTRM
//
// In other words, the F-Engine channels are divided evenly across the
// streams.
//
// The value of HNTIME is related to the heap size and HNCHAN:
//
//               spead_heap_size
//     HNTIME = -----------------
//                 4 * HNCHAN
//
// The value for "spead_heap_size" comes from the SPEAD header of each
// packet.
//
// The spead_timestamp value for a given heap is given in the SPEAD header of
// each packet.  These spead_timestamp values are ADC clock counts since the
// F-Engine sync time.  The hpguppi_daq code uses the spead_timestamp value to
// derive a related time value, known as PKTIDX, that increases by one for each
// heap.  This is done by dividing the spead_timestamp value by "HCLOCKS" (the
// number of ADC clock cycles per heap).  The value of HCLOCKS is provided by
// the coordinator via the status buffer, but it can be calculated as:
//
//     HCLOCKS = 2 * FENCHAN * heap_ntime
//
// The value of PKTIDX can be calculated as:
//
//               spead_timestamp
//     PKTIDX = -----------------
//                   HCLOCKS
//
// For a given processing pipeline instance, the SPEAD heaps from all F-Engines
// and streams that share a common spead_timestamp are referred to in this code
// as a "heapset".  The size of a heapset can be computed as:
//
//     heapset_size = spead_heap_size * NANTS * NSTRM
//
// where spead_heap_size comes from the SPEAD header of each packet, NANTS is
// externally provided via the status buffer, and NSTRM (the number of streams
// being processed by a given pipeline engine) is determined from the list of
// destination IP addresses found in the status buffer.  Given the various ways
// the F-Engine data can be distributed and combined, heapset_size varies
// accordingly.
//
// The received heaps are assembled into GUPPI RAW data blocks in the network
// thread's shared memory output data buffer.  To facilitate reuse of the
// shared memory structures, the space for block assembly is always 128 MiB.
// The heapset size may not evenly divide into the total block assembly buffer
// size, but the maximum number of complete heapsets that fit within that limit
// will go into each data block.  The BLOCSIZE parameter will be updated to
// reflect the size of the occupied portion of the shared memory block.
//
// Because each heapset has a unique PKTIDX, the number of heapsets per block
// is the same as the number of PKTIDX values per block. This value is stored
// in the status buffer as "PIPERBLK" and is calculated (using integer
// division) as:
//
//                 128 * 1024 * 1204
//     PIPERBLK = -------------------
//                   heapset_size
//
// Assuming an infinite number of data blocks, the "absolute" block number for
// a given PKTIDX value is calculated (using integer division) as:
//
//                          PKTIDX
//     abs_block_number = ----------
//                         PIPERBLK
//
// The PIPERBLK regions in a data buffer block are referred to as "slots" and
// are indexed from 0 to PIPERBLK-1.  The slot index for a given PKTIDX is
// determined by:
//
//     slot_idx = PKTIDX mod PIPERBLK
//
// For example, assuming nblocks=3 and PIPERBLK=4, the heapset for PKTIDX=93
// would go in abs_block_number=23, slot_idx=1.
//
// ## Data arrangement within a block
//
// The data in a GUPPI RAW data block are arranged as a continuous time series
// of (complex, dual-pol) samples for the first F-Engine's first stream's first
// channel (also called the "start channel" and stored in the status buffer as
// "SCHAN").  followed by the samples for the first F-Engine's first stream's
// second channel, and so on for all the channels of the first F-Engine.  This
// pattern is then repeated for all the remaining streams and F-Engines.  Thus,
// each consecutive set of "PIPERBLK * HNTIME" complex, dual-pol samples (or "4
// * PIPERBLK * HNTIME" bytes) contains all the time samples for a specific
// F-Engine and channel.
//
// To put it another way, each slot holds one heapset and is arranged as a
// 2D region of memory with a row "width" of "4 * HNTIME" bytes, a "height"
// of "HNCHAN * NSTRM * NANTS" rows, and an inter-row "stride" of "4 *
// PIPERBLK * HNTIME" bytes.
//
// The total number of frequency channels in a block is stored in the status
// buffer as OBSNCHAN and is calculated as:
//
//     OBSNCHAN = NANTS * NSTRM * HNCHAN
//
// This is NANTS sets of NSTRM * HNCHAN unique frequencies.
//
// ## Copying packet payloads into data block
//
// Each packet contains a payload corresponding to a series of bytes within a
// heap.  The location within the heap is specified by a SPEAD item (i.e. value
// from the packet header) as a heap offset, referred to here as a
// "spead_heap_offset".  `
//
// single channel (always?) and
// specifies the "spead, so each packet can be
// copied into the appropriate columns of the approriate rows in the time
// slot's 2D memory region.  Because the packets contain only a single channel
// (always?), the copy can be performed with a simple memcpy() call.  The
// tricky part is determineing the correct offset within the data block to use
// as the destination of the copy operation.
//

#ifndef _HPGUPPI_MKFENG_H_
#define _HPGUPPI_MKFENG_H_

#include <endian.h>
#include "hashpipe_packet.h"

#define SPEAD_ID_IMM_IGNORE         (0x8000)
#define SPEAD_ID_IMM_HEAP_COUNTER   (0x8001)
#define SPEAD_ID_IMM_HEAP_SIZE      (0x8002)
#define SPEAD_ID_IMM_HEAP_OFFSET    (0x8003)
#define SPEAD_ID_IMM_PAYLOAD_SIZE   (0x8004)
#define SPEAD_ID_IMM_TIMESTAMP      (0x9600)
#define SPEAD_ID_IMM_FENG_ID        (0xc101)
#define SPEAD_ID_IMM_FENG_CHAN      (0xc103)
#define SPEAD_ID_IMM_PAYLOAD_OFFSET (0x4300)

struct mk_feng_spead_info {
  uint64_t heap_counter;
  uint64_t heap_size;
  uint64_t heap_offset;
  uint64_t payload_size;
  uint64_t timestamp;
  uint64_t feng_id;
  uint64_t feng_chan;
};

// Parameters describing various data dimensions for a MeerKAT observation.
// Only schan can meaningfully be zero, all other fields should be non-zero.
// Structure is valid if all fields are non-zero except for schan which is
// valid except for -1.
struct mk_obs_info {
  // Total numner of F Engine channels
  uint32_t fenchan;
  // Total number of antennas in current subarray
  uint32_t nants;
  // Total number of streams being processed
  uint32_t nstrm;
  // Number of time samples per heap
  uint32_t hntime;
  // Number of frequency channels per heap
  uint32_t hnchan;
  // Number of ADC samples (clock cycles) per heap
  uint64_t hclocks;
  // Starting F Engine channel number to be processed
  int32_t schan;
};

#define MK_OBS_INFO_INVALID_SCHAN (-1)

// Returns the largest power of two that it less than or equal to x.
// Returns 0 if x is 0.
static inline
uint32_t
prevpow2(uint32_t x)
{
  return x == 0 ? 0 : (0x80000000 >> __builtin_clz(x));
}

// Initialize all mk_obs_info fields to invalid values
static inline
void
mk_obs_info_init(struct mk_obs_info * poi)
{
  memset(poi, 0, sizeof(struct mk_obs_info));
  poi->schan = MK_OBS_INFO_INVALID_SCHAN;
}

static inline
int
mk_obs_info_valid(const struct mk_obs_info oi)
{
  return
    (oi.fenchan != 0) &&
    (oi.nants   != 0) &&
    (oi.nstrm   != 0) &&
    (oi.hntime  != 0) &&
    (oi.hnchan  != 0) &&
    (oi.hclocks != 0) &&
    (oi.schan   != MK_OBS_INFO_INVALID_SCHAN);
}

static inline
uint32_t
spead_id(uint64_t item)
{
  return ((item >> 48) & 0xffff);
}

static inline
uint64_t
spead_imm_value(uint64_t item)
{
  return item & ((1ULL<<48) - 1);
}

static inline
uint64_t
calc_mk_pktidx(uint64_t timestamp, uint64_t hclocks)
{
  return timestamp / hclocks;
}

static inline
uint64_t
mk_pktidx(const struct mk_obs_info oi, const struct mk_feng_spead_info fesi)
{
  return calc_mk_pktidx(fesi.timestamp, oi.hclocks);
}

// For MeerKAT, the OBSNCHAN parameter represents the total number of frequency
// channels processed.  It is the number of antennas times the number of
// streams per antenna times the number of channels per heap/stream.
static inline
uint32_t
calc_mk_obsnchan(uint32_t nants, uint32_t nstrm, uint32_t hnchan)
{
  return nants * nstrm * hnchan;
}

static inline
uint32_t
mk_obsnchan(const struct mk_obs_info oi)
{
  return calc_mk_obsnchan(oi.nants, oi.nstrm, oi.hnchan);
}

// Calculate the block's channel number for a given feng_id's feng_chan.  Needs
// to know numner of streams per antenna (nstrm), number of channels per
// stream/heap (hnchan), and the start channel of the instance (schan).
static inline
uint64_t
calc_mk_block_chan(uint64_t feng_id, uint32_t nstrm,
    uint32_t hnchan, uint64_t feng_chan, uint32_t schan)
{
  return feng_id * nstrm * hnchan + (feng_chan - schan);
}

static inline
uint64_t
mk_block_chan(const struct mk_obs_info oi, const struct mk_feng_spead_info fesi)
{
  return calc_mk_block_chan(fesi.feng_id, oi.nstrm,
      oi.hnchan, fesi.feng_chan, oi.schan);
}

// Calculate the number of pktidx values per block.  Note that nchan is the
// total number of channels across all F engines.  Each PKTIDX corresponds to a
// set of heaps that all share a common timestamp. It is possible that the
// number of PKTIDX values per block (i.e. heap sets per block) times the
// number of time samples per heap will not divide the data block size evenly
// (e.g. when NANTS is not a power of two).  This means that some trailing
// bytes in the data buffer will be unoccupied.  These unoccupied bytes should
// not be processed (e.g. copied to output files) so it is important to update
// the BLOCSIZE field in the status buffer accordingly whenever a new
// pktidx_per_block value is calculated.
//
// Furthermore, the number of time samples per block is desired to be a power
// of two, so that subsequent FFTs can operate on complete blocks with maximum
// efficiency.  For this to happen, both the number of time samples per heap
// (HNTIME) and the number of PKTIDX values per block (PIPERBLK) must be powers
// of two.  HNTIME is set by the upstream F engines, so we have no control of
// that, but we can and do ensure that PIPERBLK is a power of 2.
//
// This function assumes each sample is 4 bytes ([1 real + 1 imag] * 2 pols).
static inline
uint32_t
calc_mk_pktidx_per_block(size_t block_size, uint32_t nchan, uint32_t hntime)
{
  uint32_t piperblk = (uint32_t)(block_size / (4 * nchan * hntime));
  return prevpow2(piperblk);
}

static inline
uint32_t
mk_pktidx_per_block(size_t block_size, const struct mk_obs_info oi)
{
  return calc_mk_pktidx_per_block(block_size, mk_obsnchan(oi), oi.hntime);
}

// For MeerKAT, the NTIME parameter (not stored in the status buffer or GUPPI
// RAW files), represents the total number of time samples in a block.  It
// depends on the block size and NCHAN (and sample size, but that is assumed to
// be 4 bytes).  This calculation is a bit tricky because the effective block
// size can be less than the max block size when NCHAN and HNTIME do not evenly
// divide the max block size.
static inline
uint32_t
calc_mk_ntime(size_t block_size, uint32_t nchan, uint32_t hntime)
{
  return hntime * calc_mk_pktidx_per_block(block_size, nchan, hntime);
}

static inline
uint32_t
mk_ntime(size_t block_size, const struct mk_obs_info oi)
{
  return calc_mk_ntime(block_size, mk_obsnchan(oi), oi.hntime);
}

// Calculate the effective block size for the given max block size, nchan, and
// hntime values.  The effective block size can be less than the max block size
// if nchan and/or hntime do not evenly divide the max block size.  This
// assumes 4 bytes per sample.
static inline
uint32_t
calc_mk_block_size(size_t block_size, uint32_t nchan, uint32_t hntime)
{
  return 4 * nchan * hntime * calc_mk_pktidx_per_block(block_size, nchan, hntime);
}

static inline
uint32_t
mk_block_size(size_t block_size, const struct mk_obs_info oi)
{
  return calc_mk_block_size(block_size, mk_obsnchan(oi), oi.hntime);
}

// Parses a MeerKAT F Engine packet, stores metadata in fesi, returns pointer
// to packet's spead payload.
static inline
uint8_t *
mk_parse_mkfeng_packet(const struct udppkt *p, struct mk_feng_spead_info * fesi)
{
  uint32_t i;
  uint64_t item;
  uint8_t * p_spead_payload;
  uint64_t * p_spead = (uint64_t *)p->payload;
  uint32_t nitems = be64toh(*p_spead++) & 0xffff;
  uint64_t offset = 0;

  for(i=0; i<nitems; i++) {
    item = be64toh(*p_spead++);
    switch(spead_id(item)) {
      case SPEAD_ID_IMM_HEAP_COUNTER:
        fesi->heap_counter = spead_imm_value(item);
        break;
      case SPEAD_ID_IMM_HEAP_SIZE:
        fesi->heap_size = spead_imm_value(item);
        break;
      case SPEAD_ID_IMM_HEAP_OFFSET:
        fesi->heap_offset = spead_imm_value(item);
        break;
      case SPEAD_ID_IMM_PAYLOAD_SIZE:
        fesi->payload_size = spead_imm_value(item);
        break;
      case SPEAD_ID_IMM_TIMESTAMP:
        fesi->timestamp = spead_imm_value(item);
        break;
      case SPEAD_ID_IMM_FENG_ID:
        fesi->feng_id = spead_imm_value(item);
        break;
      case SPEAD_ID_IMM_FENG_CHAN:
        fesi->feng_chan = spead_imm_value(item);
        break;
      case SPEAD_ID_IMM_PAYLOAD_OFFSET:
        offset = spead_imm_value(item);
        break;
      default:
        // Ignore
        break;
    }
  }

  p_spead_payload = ((uint8_t *)p_spead) + offset;
  return p_spead_payload;
}

#endif // _HPGUPPI_MKFENG_H_
