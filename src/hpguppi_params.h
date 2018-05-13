/* hpguppi_params.h 
 *
 * Defines structure used internally to represent observation 
 * parameters.  Includes routines to read/write this info to
 * a "FITS-style" shared memory buffer.
 */
#ifndef _GUPPI_PARAMS_H
#define _GUPPI_PARAMS_H

#include <hashpipe.h>

#include "psrfits.h"

struct hpguppi_params {
    /* Packet information for the current block */
    long long packetindex;      // Index of first packet in raw data block
    double drop_frac_avg;       // Running average of the fract of dropped packets
    double drop_frac_tot;       // Total fraction of dropped packets
    double drop_frac;           // Fraction of dropped packets in this block
    int packetsize;             // Size in bytes of data portion of each packet
    int n_packets;              // Total number of packets in current block
    int n_dropped;              // Number of packets dropped in current block
    int packets_per_block;      // Total number of packets per block
    int stt_valid;              // Has an accurate start time been measured
    long long start_pkt;        // Packet number at start of a recording
    /* Backend hardware info */
    int decimation_factor;      // Number of raw spectra integrated
    int n_bits_adc;             // Number of bits sampled by ADCs
    int pfb_overlap;            // PFB overlap factor
    int coherent;               // True if coherent mode is in use.
    float scale[16*1024];       // Per-channel scale factor
    float offset[16*1024];      // Per-channel offset
};

#if 0
void guppi_read_obs_mode(const char *buf, char *mode);
#endif
void hpguppi_read_net_params(char *buf, struct hashpipe_udp_params *u);
void hpguppi_read_subint_params(char *buf, 
                                struct hpguppi_params *g, 
                                struct psrfits *p);
void hpguppi_read_obs_params(char *buf, 
                             struct hpguppi_params *g, 
                             struct psrfits *p);
void hpguppi_free_psrfits(struct psrfits *p);

struct hpguppi_pktsock_params {
    /* Info needed from outside: */
    char ifname[80];  /* Local interface name (e.g. "eth4") */
    int port;         /* UDP receive port */
    size_t packet_size;     /* Expected packet size, 0 = don't care */
    char packet_format[32]; /* Packet format */
    // obsschan is the first coarse channel for this instance.
    // obsnchan is the total number of channels (per beam) for this instance.
    // In "mb1" mode, the `obsnchan` channels are split across 8 packets.
    int obsschan; /* First coarse channel of this instance */
    int obsnchan; /* Total number of coarse channels for this instance */
    int chperpkt; /* Total number of coarse channels per packet */

    // Holds packet socket details
    struct hashpipe_pktsock ps;
};

// Read networking parameters for packet sockets.  Same as for UDP sockets,
// though DATAHOST should be local interface name (e.g. eth4) rather than
// remote host name.
void hpguppi_read_pktsock_params(char *buf, struct hpguppi_pktsock_params *p);

// Read direct I/O mode.  This comes from the DIRECTIO status buffer key.  It
// is intereted as a number.  Defined and non-zero numeric (e.g. 1) means to
// use Direct I/O.  Undefined or zero or non-numeric means do NOT use Direct
// I/O.
int hpguppi_read_directio_mode(char *buf);

// Calculate the largest power of two number of time samples that fit in
// max_block_size block size for a given number of channels.
int calc_ntime_per_block(int max_block_size, int obsnchan);
#endif
