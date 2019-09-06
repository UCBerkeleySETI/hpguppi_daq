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
// Each heap contains "heap_ntime" time samples for a given frequency channel
// followed by heap_ntime time samples for the next frequency channel and so on
// for all the channels of that heap's stream.  Each heap contains data from a
// single F-Engine.
//
// The number of channels per heap, "heap_nchan" is given by:
//
//                   FENCHAN
//     heap_nchan = ---------
//                   FENSTRM
//
// In other words, the F-Engine channels are divided evenly across the
// streams.
//
// The value of "heap_ntime" can be derived by:
//
//                   spead_payload_size
//     heap_ntime = --------------------
//                     4 * heap_nchan
//
// The value for "spead_payload_size" comes from the SPEAD header of each
// packet.
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
// shared memory structures, the assembled block size is always 128 MiB.  The
// number of unique spead_timestamp values that span a block is known as
// "block_ntime" and depends on the size of each heapset.  The value of
// block_ntime can be calculated as:
//
//                    128 * 1024 * 1204
//     block_ntime = -------------------
//                      heapset_size
//
// The spead_timestamp value for a given heap is given in the SPEAD header of
// each packet.  These spead_timestamp values are ADC clock counts since the
// F-Engine sync time.  The hpguppi_daq code uses the spead_timestamp value to
// derive a related time value, known as PKTIDX, that increases by one for each
// heap  (and heapset).  This is done by dividing the spead_timestamp value by
// "adc_clocks_per_heap" (the number of ADC clock cycles per heap).  The value
// of adc_clocks_per_heap can be calculated as:
//
//     adc_clocks_per_heap = 2 * FENCHAN * heap_ntime
//
// The value of PKTIDX can then be calculated as:
//
//                 spead_timestamp
//     PKTIDX = ---------------------
//               adc_clocks_per_heap
//
// As described above, each GUPPI RAW data block contains block_ntime unique
// spead_timestamp values.  Equivalently, each data block contains block_ntime
// heapsets.  These block_ntime regions in the data buffer are referred to as
// "time slots" and are indexed from 0 to block_ntime-1.  The time slot index
// for a given PKTIDX is determined by simply by:
//
//     time_slot_idx = PKTIDX mod block_ntime
//
// ## Data block arrangement
//
// The data in a GUPPI RAW data block are arranged as a continuous time series
// of (complex, dual-pol) samples for the first F-Engine's first channel,
// followed by the samples for the first F-Engine's second channel, and so on
// for all the channels of the first F-Engine.  This pattern is then repeated
// for all the remaining F-Engines.  Thus, each consecutive set of "block_ntime
// * heap_ntime" (complex, dual-pol) samples (or "4 * block_time * heap_time"
// bytes) contains all the timesamples for a specific F-Engine and channel.
//
// To put it another way, each time slot holds one heapset and is arranged as a
// 2D region of memory with a row "width" of "4 * heap_ntime" bytes, a "height"
// of "heap_nchan * NSTRM * NANTS" rows, and an inter-row "stride" of "4 *
// block_time * heap_time" bytes.
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
  return (item >> 48);
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

// For MeerKAT, the NCHAN parameter represents the total number of frequency
// channels processed.  It is the number of antennas times the number of
// streams per antenna times the number of channels per heap/stream.
static inline
uint32_t
calc_mk_nchan(uint32_t nants, uint32_t nstrm, uint32_t hnchan)
{
  return nants * nstrm * hnchan;
}

static inline
uint32_t
mk_nchan(const struct mk_obs_info oi)
{
  return calc_mk_nchan(oi.nants, oi.nstrm, oi.hnchan);
}

// Calculate the block's channel number for a given feng_id's feng_chan.  Needs
// to know numner of streams per antenna (nstrm), number of channels per
// stream/heap (hnchan), and the start channel of the instance (schan).
static inline
uint64_t
calc_mk_block_chan(uint64_t feng_id, uint32_t nstrm,
    uint32_t hnchan, uint64_t feng_chan, uint32_t schan)
{
  return feng_chan * nstrm * hnchan + (feng_chan - schan);
}

static inline
uint64_t
mk_block_chan(const struct mk_obs_info oi, const struct mk_feng_spead_info fesi)
{
  return calc_mk_block_chan(fesi.feng_chan, oi.nstrm,
      oi.hnchan, fesi.feng_chan, oi.schan);
}

// Calculate the number of pktidx values per block.
// Assumes sample size is 4 bytes.
static inline
uint32_t
calc_mk_pktidx_per_block(size_t block_size, uint32_t nchan, uint32_t hntime)
{
  return (uint32_t)(block_size / (4 * nchan * hntime));
}

static inline
uint32_t
mk_pktidx_per_block(size_t block_size, const struct mk_obs_info oi)
{
  return calc_mk_pktidx_per_block(block_size, mk_nchan(oi), oi.hntime);
}

// For MeerKAT, the NTIME parameter (not stored in the status buffer or GUPPI
// RAW files), represents the total number of time samples in a block.
// It depends on the block size and NCHAN (and sample size, but that is assumed
// to be 4 bytes).
static inline
uint32_t
calc_mk_ntime(size_t block_size, uint32_t nchan)
{
  return (uint32_t)(block_size / (4 * nchan));
}

static inline
uint32_t
mk_ntime(size_t block_size, const struct mk_obs_info oi)
{
  return calc_mk_ntime(block_size, mk_nchan(oi));
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
    item = *p_spead++;
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
