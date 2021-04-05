/* hpguppi_udp.h
 *
 * Functions dealing with GUPPI UDP packets.
 */
#ifndef _GUPPI_UDP_H
#define _GUPPI_UDP_H

#include <sys/types.h>
#include <netdb.h>
#include <poll.h>
#include <netinet/if_ether.h>
#include <netinet/ip.h>
#include <netinet/udp.h>

// DIBAS/VEGAS CODD-mode format UDP packet payload consists of 8 bytes header +
// 8192 bytes data + 24 bytes footer.
//
// Packet offset to payload header
#define PKT_OFFSET_GUPPI_PAYLOAD_HEADER \
  (sizeof(struct ethhdr) + \
   sizeof(struct iphdr ) + \
   sizeof(struct udphdr))

// Size of payload header
#define PKT_SIZE_GUPPI_PAYLOAD_HEADER (8)

// Packet offset to the payload data
#define PKT_OFFSET_GUPPI_PAYLOAD_DATA \
  (PKT_OFFSET_GUPPI_PAYLOAD_HEADER + PKT_SIZE_GUPPI_PAYLOAD_HEADER)

// Size of payload data
#define PKT_SIZE_GUPPI_PAYLOAD_DATA (8192)

// Packet offset to the payload data
#define PKT_OFFSET_GUPPI_PAYLOAD_FOOTER \
  (PKT_OFFSET_GUPPI_PAYLOAD_DATA + PKT_SIZE_GUPPI_PAYLOAD_DATA)

// Payload footer is 8 bytes "frame check sequence" (unused) + two 8 byte
// "Inter-Frame Gaps" (not sure why those were added).
#define PKT_SIZE_GUPPI_PAYLOAD_FOOTER (8+8+8)

#define GUPPI_MAX_PACKET_SIZE 9000

/* Struct to hold connection parameters */
struct hpguppi_udp_params {

    /* Info needed from outside: */
    char sender[80];  /* Sender hostname */
    int port;         /* Receive port */
    size_t packet_size;     /* Expected packet size, 0 = don't care */
    char packet_format[32]; /* Packet format */

    /* Derived from above: */
    int sock;                       /* Receive socket */
    struct addrinfo sender_addr;    /* Sender hostname/IP params */
    struct pollfd pfd;              /* Use to poll for avail data */
};

/* Basic structure of a packet.  This struct, functions should 
 * be used to get the various components of a data packet.   The
 * internal packet structure is:
 *   1. sequence number (64-bit unsigned int)
 *   2. data bytes (typically 8kB)
 *   3. status flags (64 bits)
 *
 * Except in the case of "1SFA" packets:
 *   1. sequence number (64b uint)
 *   2. data bytes (typically 8128B)
 *   3. status flags (64b)
 *   4. blank space (16B)
 */
struct hpguppi_udp_packet {
    size_t packet_size;  /* packet size, bytes */
    char data[GUPPI_MAX_PACKET_SIZE]; /* packet data */
};
unsigned long long hpguppi_udp_packet_seq_num(const struct hpguppi_udp_packet *p);
unsigned long long hpguppi_udp_packet_seq_num_from_payload(const char *payload);
const char *hpguppi_udp_packet_data(const struct hpguppi_udp_packet *p);
const char *hpguppi_udp_packet_data_from_payload(const char *payload, size_t payload_size);
size_t hpguppi_udp_packet_datasize(size_t packet_size);
size_t parkes_udp_packet_datasize(size_t packet_size);
unsigned long long hpguppi_udp_packet_flags(const struct hpguppi_udp_packet *p);
unsigned long long hpguppi_udp_packet_flags_from_payload(const char *payload, size_t payload_size);

/* Use sender and port fields in param struct to init
 * the other values, bind socket, etc.
 */
int hpguppi_udp_init(struct hpguppi_udp_params *p);

/* Wait for available data on the UDP socket */
int hpguppi_udp_wait(struct hpguppi_udp_params *p); 

/* Read a packet */
int hpguppi_udp_recv(struct hpguppi_udp_params *p, struct hpguppi_udp_packet *b);

/* Convert a Parkes-style packet to a GUPPI-style packet */
void parkes_to_guppi(struct hpguppi_udp_packet *b, const int acc_len, 
        const int npol, const int nchan);
void parkes_to_guppi_from_payload(char *payload, const int acc_len, 
        const int npol, const int nchan);

/* Copy a guppi packet to the specified location in memory, 
 * expanding out missing channels for 1SFA packets.
 */
void hpguppi_udp_packet_data_copy(char *out, const struct hpguppi_udp_packet *p);
void hpguppi_udp_packet_data_copy_from_payload(char *out, const char *payload, size_t payload_size);

/* Generic(ish) transpose copy function */
void hpguppi_data_copy_transpose(char *out, const char* in,
    const unsigned chan_per_packet,
    const unsigned samp_per_packet,
    const unsigned samp_per_block);

/* AVX-optimized version of generic(ish) transpose copy function */
void hpguppi_data_copy_transpose_avx(char *out, const char* in,
    const unsigned chan_per_packet,
    const unsigned samp_per_packet,
    const unsigned samp_per_block);

#if 0
// Inefficient!
#if HAVE_AVX512F_INSTRUCTIONS
/* AVX512 version of generic(ish) transpose copy function */
void hpguppi_data_copy_transpose_avx512(char *out, const char* in,
    const unsigned chan_per_packet,
    const unsigned samp_per_packet,
    const unsigned samp_per_block);
#endif // HAVE_AVX512F_INSTRUCTIONS
#endif // 0 (inefficient)

/* Copy and corner turn for baseband multichannel modes */
void hpguppi_udp_packet_data_copy_transpose(char *databuf, int nchan,
        unsigned block_pkt_idx, unsigned packets_per_block,
        const struct hpguppi_udp_packet *p);
void hpguppi_udp_packet_data_copy_transpose_from_payload(char *databuf, int nchan,
        unsigned block_pkt_idx, unsigned packets_per_block,
        const char *payload, size_t payload_size);
void hpguppi_s6_packet_data_copy_from_payload(char *databuf, int block_chan,
        unsigned block_time, unsigned obsnchan,
        const char *payload, size_t payload_size);
void hpguppi_s6_packet_data_copy_transpose_from_payload(char *databuf, int block_chan,
        unsigned block_time, unsigned ntime_per_block,
        const char *payload, size_t payload_size);
void hpguppi_s6mb_packet_data_copy_from_payload(char *databuf, int block_chan,
        unsigned block_time, unsigned ntime_per_block,
        const char *payload, size_t payload_size);

/* Close out socket, etc */
int hpguppi_udp_close(struct hpguppi_udp_params *p);

#endif
