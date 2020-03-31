// hpguppi_ibverbs_pkt_thread.h
//
// Header file for externally visible data structures and functions in
// hpguppi_ibverbs_pkt_thread.c.

#ifndef _HPGUPPI_IBVERBS_PKT_THREAD_H_
#define _HPGUPPI_IBVERBS_PKT_THREAD_H_

#include "hashpipe_ibverbs.h"
#include "hpguppi_databuf.h"

// Alignment size to use.  Currently set to 64 (== 512/8) for compatibility
// with AVX512 instructions.
#define PKT_ALIGNMENT_SIZE (64)

// Maximum number of chunks supported
#define MAX_CHUNKS (8)

// Structure that holds info about a "chunk".  A chunk is part of a packet that
// is stored at a PKT_ALIGNMENT_SIZE aligned address.  The chunk_size is the
// number of bytes from the packet that are stored in the chunk.  The
// chunk_aligned_size is chunk_size rounded up to the next multple of
// PKT_ALIGNMENT_SIZE.  The chunk_offset is the offset of the chunk from the
// packet's first chunk.  The first chunk will have a chunk_offset of 0.
struct hpguppi_pktbuf_chunk {
  size_t chunk_size;
  size_t chunk_aligned_size;
  off_t chunk_offset;
};

// Structure that holds info about packet/slot/block sizing.  A block is
// divided into "slots".  Each slot holds one packet, possibly with internal
// and/or trailing padding added to align various sections of the packet to
// PKT_ALIGNMENT_SIZE.  These sections are called chunks.  num_chunks specifies
// the number of chunks that are being used.  The slot_size is the size of a
// slot and equals the sum of the chunk_aligned_sizes.  slots_per_block is the
// number of slots in a data block.  Note that slot_size * slots_per_block may
// be less than the size of data block by up PKT_ALIGNMENT_SIZE-1 bytes.
struct hpguppi_pktbuf_info {
  uint32_t num_chunks;
  size_t slot_size;
  size_t slots_per_block;
  struct hpguppi_pktbuf_chunk chunks[MAX_CHUNKS];
};

// Function to get a pointer to a databuf's pktbuf_info structure.  Assumes
// that the pktbuf_info structure is tucked into the "padding" bytes of the
// hpguppi_intput_databuf.
// TODO Check db->header.data_type?
static inline
struct hpguppi_pktbuf_info *
hpguppi_pktbuf_info_ptr(hpguppi_input_databuf_t *db)
{
  return (struct hpguppi_pktbuf_info *)(db->padding);
}

// `hpguppi_ibvpkt_flow() is used to setup flow rules on the NIC to
// select which incoming packets will be passed to us by the NIC.  Flows are
// specified by providing values that various fields in the packet headers must
// match.  Fields that can be matched exist at the Ethernet level, the IPv4
// level, and the TCP/UDP level.  The fields available for matching are:
//
//   - dst_mac    Ethernet destination MAC address (uint8_t *)
//   - src_mac    Ethernet source MAC address      (uint8_t *)
//   - ether_type Ethernet type field              (uint16_t)
//   - vlan_tag   Ethernet VLAN tag                (uint16_t)
//   - src_ip     IP source address                (uint32_t)
//   - dst_ip     IP destination address           (uint32_t)
//   - src_port   TCP/UDP source port              (uint16_t)
//   - dst_port   TCP/UDP destination port         (uint16_t)
//
// The `flow_idx` parameter specifies which flow rule to assign this flow to.
// The user specifies `max_flows` when initializing the `hashpipe_ibv_context`
// structure and `flow_idx` must be less than that number.  If a flow already
// exists at the index `flow_idx`, that flow is destroyed before the new flow
// is created and stored at the same index.
//
// The `flow_type` field specifies the type of the flow.  Supported values are:
//
// IBV_FLOW_SPEC_ETH   This matches packets only at the Ethernet layer.  Match
//                     fields for IP/TCP/UDP are ignored.
//
// IBV_FLOW_SPEC_IPV4  This matches at the Ethernet and IPv4 layers.  Match
//                     fields for TCP/UDP are ignored.  Flow rules at this
//                     level include an implicit match on the Ethertype field
//                     (08 00) to select only IP packets.
//
// IBV_FLOW_SPEC_TCP   These match at the Ethernet, IPv4, and TCP/UDP layers.
// IBV_FLOW_SPEC_UDP   Flow rules of these types include an implicit match on
//                     the Ethertype field to select only IP packets and the IP
//                     protocol field to select only TCP or UDP packets.
//
// Not all fields need to be matched.  For fields for which a match is not
// desired, simply pass NULL or 0 for the corresponding parameter to
// `hashpipe_ibv_flow` and that field will be excluded from the matching
// process.  This means that it is not possible to match against zero valued
// fields except for the bizarre case of a zero valued MAC address.  In
// practice this is unlikely to be a problem.
//
// Passing NULL/0 for all the match fields will result in the destruction of
// any flow at the `flow_idx` location, but no new flow will be stored there.
//
// The `src_mac` and `dst_mac` pointers, if non-NULL, must point to a 6 byte
// buffer containing the desired MAC address in network byte order.  Note that
// the `mac` field of `hibv_ctx` will contain the MAC address of the NIC port
// being used.  Some NICs may require a `dst_mac` match in order to enable any
// packet reception at all.  This can be the unicast MAC address of the NIC
// port or a multicast Ethernet MAC address for receiving multicast packets.
// If a multicast `dst_ip` is given, `dst_mac` will be ignored and the
// multicast MAC address corresponding to `dst_ip` will be used.  If desired,
// multicast MAC addresses can be generated from multicast IP addresses using
// the ETHER_MAP_IP_MULTICAST macro defined in <netinet/if_ether.h>.
//
// The non-MAC  parameters are passed as values and must be in host byte order.
//
// This function is provided to allow downstream threads to setup flows without
// exposing the underlying hashpipe_ibv_context structure.  To ensure that the
// underlying hashpipe_ibv_context structure has been fully initialized,
// downstream threads should call TODO before calling this function.  To ensure
// that downstream threads don't collide with each other while managing flows,
// it is recommended that they only call this functio while they haver the
// status buffer locked.
int hpguppi_ibvpkt_flow(
    hpguppi_input_databuf_t *db,
    uint32_t  flow_idx,   enum ibv_flow_spec_type flow_type,
    uint8_t * dst_mac,    uint8_t * src_mac,
    uint16_t  ether_type, uint16_t  vlan_tag,
    uint32_t  src_ip,     uint32_t  dst_ip,
    uint16_t  src_port,   uint16_t  dst_port);

// Function that threads can call to wait for hpguppi_ibvpkt_thread to finalize
// ibverbs setup.  After this funtion returns, the underlying
// hashpipe_ibv_context structure used by hpguppi_ibvpkt_thread will be fully
// initialized and flows can be created/destroyed by calling
// hpguppi_ibvpkt_flow().
void hpguppi_ibvpkt_wait_running(hashpipe_status_t * st);

// Function to get a pointer to slot "slot_id" in block "block_id" of databuf
// "db".
static inline
uint8_t *
hpguppi_pktbuf_block_slot_ptr(hpguppi_input_databuf_t *db,
    uint64_t block_id, uint32_t slot_id)
{
  struct hpguppi_pktbuf_info * pktbuf_info = hpguppi_pktbuf_info_ptr(db);
  return (uint8_t *)db->block[block_id].data + slot_id * pktbuf_info->slot_size;
}

#endif // _HPGUPPI_IBVERBS_PKT_THREAD_H_
