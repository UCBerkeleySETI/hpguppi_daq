/* hpguppi_udp.c
 *
 * GUPPI UDP packet implementations.
 */
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <fcntl.h>
#include <poll.h>

#include <immintrin.h>

#include <hashpipe.h>

#include "hpguppi_databuf.h"
#include "hpguppi_udp.h"
#include "transpose.h"


int hpguppi_udp_init(struct hpguppi_udp_params *p) {

    /* Resolve sender hostname */
    struct addrinfo hints;
    struct addrinfo *result, *rp;
    memset(&hints, 0, sizeof(struct addrinfo));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_DGRAM;
    int rv = getaddrinfo(p->sender, NULL, &hints, &result);
    if (rv!=0) { 
        hashpipe_error(__FUNCTION__, "getaddrinfo failed");
        return(HASHPIPE_ERR_SYS);
    }

    /* Set up socket */
    p->sock = socket(PF_INET, SOCK_DGRAM, 0);
    if (p->sock==-1) { 
        hashpipe_error(__FUNCTION__, "socket error");
        freeaddrinfo(result);
        return(HASHPIPE_ERR_SYS);
    }

    /* bind to local address */
    struct sockaddr_in local_ip;
    local_ip.sin_family =  AF_INET;
    local_ip.sin_port = htons(p->port);
    local_ip.sin_addr.s_addr = INADDR_ANY;
    rv = bind(p->sock, (struct sockaddr *)&local_ip, sizeof(local_ip));
    if (rv==-1) {
        hashpipe_error(__FUNCTION__, "bind");
        return(HASHPIPE_ERR_SYS);
    }

    /* Set up socket to recv only from sender */
    for (rp=result; rp!=NULL; rp=rp->ai_next) {
        if (connect(p->sock, rp->ai_addr, rp->ai_addrlen)==0) { break; }
    }
    if (rp==NULL) { 
        hashpipe_error(__FUNCTION__, "connect error");
        close(p->sock); 
        freeaddrinfo(result);
        return(HASHPIPE_ERR_SYS);
    }
    memcpy(&p->sender_addr, rp, sizeof(struct addrinfo));
    freeaddrinfo(result);

    /* Non-blocking recv */
    fcntl(p->sock, F_SETFL, O_NONBLOCK);

    /* Increase recv buffer for this sock */
    int bufsize = 128*1024*1024;
    socklen_t ss = sizeof(int);
    rv = setsockopt(p->sock, SOL_SOCKET, SO_RCVBUF, &bufsize, sizeof(int));
    if (rv<0) { 
        hashpipe_error(__FUNCTION__, "Error setting rcvbuf size.");
        perror("setsockopt");
    } 
    rv = getsockopt(p->sock, SOL_SOCKET, SO_RCVBUF, &bufsize, &ss); 
    if (0 && rv==0) { 
        printf("hpguppi_udp_init: SO_RCVBUF=%d\n", bufsize);
    }

    /* Poll command */
    p->pfd.fd = p->sock;
    p->pfd.events = POLLIN;

    return(HASHPIPE_OK);
}

int hpguppi_udp_wait(struct hpguppi_udp_params *p) {
    int rv = poll(&p->pfd, 1, 1000); /* Timeout 1sec */
    if (rv==1) { return(HASHPIPE_OK); } /* Data ready */
    else if (rv==0) { return(HASHPIPE_TIMEOUT); } /* Timed out */
    else { return(HASHPIPE_ERR_SYS); }  /* Other error */
}

int hpguppi_udp_recv(struct hpguppi_udp_params *p, struct hpguppi_udp_packet *b) {
    int rv = recv(p->sock, b->data, GUPPI_MAX_PACKET_SIZE, 0);
    b->packet_size = rv;
    if (rv==-1) { return(HASHPIPE_ERR_SYS); }
    else if (p->packet_size) {
        if (rv!=p->packet_size) { return(HASHPIPE_ERR_PACKET); }
        else { return(HASHPIPE_OK); }
    } else { 
        p->packet_size = rv;
        return(HASHPIPE_OK); 
    }
}

unsigned long long change_endian64(const unsigned long long *d) {
    unsigned long long tmp;
    char *in=(char *)d, *out=(char *)&tmp;
    int i;
    for (i=0; i<8; i++) {
        out[i] = in[7-i];
    }
    return(tmp);
}

unsigned long long hpguppi_udp_packet_seq_num(const struct hpguppi_udp_packet *p)
{
    return hpguppi_udp_packet_seq_num_from_payload(p->data);
}

unsigned long long hpguppi_udp_packet_seq_num_from_payload(const char *payload)
{
    // XXX Temp for new baseband mode, blank out top 8 bits which
    // contain channel info.
    unsigned long long tmp = change_endian64((unsigned long long *)payload);
    tmp &= 0x00FFFFFFFFFFFFFF;
    return(tmp);
}

#define PACKET_SIZE_ORIG ((size_t)8208)
#define PACKET_SIZE_SHORT ((size_t)544)
#define PACKET_SIZE_1SFA ((size_t)8224)
#define PACKET_SIZE_1SFA_OLD ((size_t)8160)
#define PACKET_SIZE_FAST4K ((size_t)4128)
#define PACKET_SIZE_PASP ((size_t)528)

size_t hpguppi_udp_packet_datasize(size_t packet_size) {
    /* Special case for the new "1SFA" packets, which have an extra
     * 16 bytes at the end reserved for future use.  All other guppi
     * packets have 8 bytes index at the front, and 8 bytes error
     * flags at the end.
     * NOTE: This represents the "full" packet output size...
     */
    if (packet_size==PACKET_SIZE_1SFA) // 1SFA packet size
        return((size_t)8192);
    else if (packet_size==PACKET_SIZE_SHORT) 
        //return((size_t)256);
        return((size_t)512);
    else              
        return(packet_size - 2*sizeof(unsigned long long));
}

const char *hpguppi_udp_packet_data(const struct hpguppi_udp_packet *p)
{
    return hpguppi_udp_packet_data_from_payload(p->data, p->packet_size);
}

const char *hpguppi_udp_packet_data_from_payload(const char *payload, size_t payload_size)
{
    /* This is valid for all guppi packet formats
     * PASP has 16 bytes of header rather than 8.
     */
    if (payload_size==PACKET_SIZE_PASP)
        return(payload + (size_t)16);
    return(payload + sizeof(unsigned long long));
}

unsigned long long hpguppi_udp_packet_flags(const struct hpguppi_udp_packet *p)
{
    return hpguppi_udp_packet_flags_from_payload(p->data, p->packet_size);
}

unsigned long long hpguppi_udp_packet_flags_from_payload(const char *payload, size_t payload_size)
{
    return(*(unsigned long long *)((char *)(payload)
                + payload_size - sizeof(unsigned long long)));
}


/* Copy the data portion of a guppi udp packet to the given output
 * address.  This function takes care of expanding out the 
 * "missing" channels in 1SFA packets.
 */
void hpguppi_udp_packet_data_copy(char *out, const struct hpguppi_udp_packet *p)
{
    return hpguppi_udp_packet_data_copy_from_payload(out, p->data, p->packet_size);
}

void hpguppi_udp_packet_data_copy_from_payload(char *out, const char *payload, size_t payload_size)
{
    const char *packet_data = hpguppi_udp_packet_data_from_payload(payload, payload_size);

    if (payload_size==PACKET_SIZE_1SFA_OLD) {
        /* Expand out, leaving space for missing data.  So far only
         * need to deal with 4k-channel case of 2 spectra per packet.
         * May need to be updated in the future if 1SFA works with
         * different numbers of channels.
         *
         * TODO: Update 5/12/2009, newer 1SFA modes always will have full
         * data contents, and the old 4k ones never really worked, so
         * this code can probably be deleted.
         */
        const size_t pad = 16;
        const size_t spec_data_size = 4096 - 2*pad;
        memset(out, 0, pad);
        memcpy(out + pad, packet_data, spec_data_size);
        memset(out + pad + spec_data_size, 0, 2*pad);
        memcpy(out + pad + spec_data_size + pad + pad,
                packet_data + spec_data_size,
                spec_data_size);
        memset(out + pad + spec_data_size + pad
                + pad + spec_data_size, 0, pad);
    } else {
        /* Packet has full data, just do a memcpy */
        memcpy(out, packet_data,
                hpguppi_udp_packet_datasize(payload_size));
    }
}

/* Copy function for baseband data that does a partial
 * corner turn (or transpose) based on nchan.  In this case
 * out should point to the beginning of the data buffer.
 * block_pkt_idx is the seq number of this packet relative
 * to the beginning of the block.  packets_per_block
 * is the total number of packets per data block (all channels).
 */
void hpguppi_udp_packet_data_copy_transpose(char *databuf, int nchan,
        unsigned block_pkt_idx, unsigned packets_per_block,
        const struct hpguppi_udp_packet *p)
{
    return hpguppi_udp_packet_data_copy_transpose_from_payload(databuf, nchan,
            block_pkt_idx, packets_per_block,
            p->data, p->packet_size);
}

// - `out` should point to first destination location for packet (i.e. time
//   dependent offset into first channel).
// - `in` should point to first byte of payload data
// - `chan_per_packet` is number of channels per packet
// - `samp_per_packet` is number of time (spectra) samples per packet
// - `samp_per_block` is number of time (spectra) samples per block
void hpguppi_data_copy_transpose(char *out, const char* in,
    const unsigned chan_per_packet,
    const unsigned samp_per_packet,
    const unsigned samp_per_block)
{
    const size_t bytes_per_sample = 4;
    const char *iptr;
    unsigned isamp, ichan;

    // Arrange data from network packet format e.g:
    // S0C0P0123, S0C1P0123, S0C2P0123, ... S0CnP0123 <== Time (spectra) 0
    // S1C0P0123, S1C1P0123, S1C2P0123, ... S1CnP0123 <== Time (spectra) 1
    // S2C0P0123, S2C1P0123, S3C2P0123, ... S2CnP0123 <== Time (spectra) 2
    // S3C0P0123, S3C1P0123, S3C2P0123, ... S3CnP0123 <== Time (spectra) 3
    // ...
    // SmC0P0123, SmC1P0123, SmC2P0123, ... SmCnP0123 <== Time (spectra) m

    // Into the format:
    // S0C0P0123, S1C0P0123, S2C0P0123, ... SmC0P0123 <== Chan 0
    // S0C1P0123, S1C1P0123, S2C1P0123, ... SmC1P0123 <== Chan 1
    // S0C2P0123, S1C2P0123, S2C2P0123, ... SmC2P0123 <== Chan 2
    // S0C3P0123, S1C3P0123, S2C3P0123, ... SmC3P0123 <== Chan 3
    // ...
    // S0CnP0123, S1CnP0123, S2CnP0123, ... SmCnP0123 <== Chan n

    /* New improved more cache friendly version on CPU */
    for (ichan=0; ichan<chan_per_packet; ++ichan)
    {
        iptr = in + (ichan*bytes_per_sample);
        for (isamp=0; isamp<samp_per_packet; ++isamp)
        {
            memcpy(out, iptr, bytes_per_sample);
            out += bytes_per_sample;
            iptr += chan_per_packet*bytes_per_sample;
        }
        out += bytes_per_sample*(samp_per_block-samp_per_packet);
    }
}

// - `out` should point to first destination location for packet (i.e. time
//   dependent offset into first channel).
// - `in` should point to first byte of payload data
// - `chan_per_packet` is number of channels per packet
// - `samp_per_packet` is number of time (spectra) samples per packet
// - `samp_per_block` is number of time (spectra) samples per block
void hpguppi_data_copy_transpose_avx(char *out, const char* in,
    const unsigned chan_per_packet,
    const unsigned samp_per_packet,
    const unsigned samp_per_block)
{
    // Arrange data from network packet format e.g:
    //
    //     S0C0P0123, S0C1P0123, ..., S0CnP0123 <== Time (spectra) 0
    //     S1C0P0123, S1C1P0123, ..., S1CnP0123 <== Time (spectra) 1
    //     ...
    //     SmC0P0123, SmC1P0123, ..., SmCnP0123 <== Time (spectra) m
    //
    // Into the format:
    //
    //     S0C0P0123, S1C0P0123, ..., SmC0P0123 <== Chan 0
    //     S0C1P0123, S1C1P0123, ..., SmC1P0123 <== Chan 1
    //     ...
    //     S0CnP0123, S1CnP0123, ..., SmCnP0123 <== Chan n
    //
    // Use _mm256_i32gather_epi32 instrinsic to load 8 x 32 bit values from
    // strided locations in source `pin`, then use _mm256_stream_si256
    // instrinsic to store 8 x 32 bit values in destrination `pout`.  The first
    // load will get values:
    //
    //     S0C0P0123 <== Time (spectra) 0
    //     S1C0P0123 <== Time (spectra) 1
    //     ...
    //     S7C0P0123 <== Time (spectra) 3
    //
    // These values will be stored as:
    //
    //     S0C0P0123m S1C0P0123, ..., S7C0P0123

    int c, t;
    float *pfsrc;
    float *pfdst;

    // Outer loop steps by 8 frequency channels
    // Inner loop steps by 8 time samples
    // Nesting the loops this way was found to be much faster than the other
    // way around (at least on "Intel(R) Xeon(R) CPU E5-2620 v3 @ 2.40GHz").
    for(c=0; c<chan_per_packet; c+=8) {
        for(t=0; t<samp_per_packet; t+=8) {
            pfsrc = ((float *)in) + t*chan_per_packet + c;
            pfdst = ((float *)out) + c*samp_per_block + t;
            transpose_8x8_f32(pfdst, pfsrc, chan_per_packet, samp_per_block);
        }
    }
}

#if 0
// WARNING!!! This is not efficient.  AVX512 optimized copy-with-transpose
// WARNING!!! should use a transpose_16x16 function analogous to the
// WARNING!!! transpose_8x8_f32 function used in the AVX version.

#if HAVE_AVX512F_INSTRUCTIONS
// - `out` should point to first destination location for packet (i.e. time
//   dependent offset into first channel).
// - `in` should point to first byte of payload data
// - `chan_per_packet` is number of channels per packet
// - `samp_per_packet` is number of time (spectra) samples per packet
// - `samp_per_block` is number of time (spectra) samples per block
void hpguppi_data_copy_transpose_avx512(char *out, const char* in,
    const unsigned chan_per_packet,
    const unsigned samp_per_packet,
    const unsigned samp_per_block)
{
    // Arrange data from network packet format e.g:
    //
    //     S0C0P0123, S0C1P0123, ..., S0CnP0123 <== Time (spectra) 0
    //     S1C0P0123, S1C1P0123, ..., S1CnP0123 <== Time (spectra) 1
    //     ...
    //     SmC0P0123, SmC1P0123, ..., SmCnP0123 <== Time (spectra) m
    //
    // Into the format:
    //
    //     S0C0P0123, S1C0P0123, ..., SmC0P0123 <== Chan 0
    //     S0C1P0123, S1C1P0123, ..., SmC1P0123 <== Chan 1
    //     ...
    //     S0CnP0123, S1CnP0123, ..., SmCnP0123 <== Chan n
    //
    // Use _mm512_i32gather_epi32 instrinsic to load 16 x 32 bit values from
    // strided locations in source `pin`, then use _mm512_stream_si512
    // instrinsic to store 16 x 32 bit values in destrination `pout`.  The first
    // load will get values:
    //
    //     S0C0P0123 <== Time (spectra) 0
    //     S1C0P0123 <== Time (spectra) 1
    //     ...
    //     S15C0P0123 <== Time (spectra) 3
    //
    // These values will be stored as:
    //
    //     S0C0P0123m S1C0P0123, ..., S15C0P0123
    //
    // Outer loop steps through input by 16 time samples, inner loop steps
    // through input by 1 frequency channel.

    const int * pin = (const int *)in;
    __m512i * pout = (__m512i *)out;


    __m512i data;
    __m512i vindex = _mm512_set_epi32(
        15 * chan_per_packet,
        14 * chan_per_packet,
        13 * chan_per_packet,
        12 * chan_per_packet,
        11 * chan_per_packet,
        10 * chan_per_packet,
        9 * chan_per_packet,
        8 * chan_per_packet,
        7 * chan_per_packet,
        6 * chan_per_packet,
        5 * chan_per_packet,
        4 * chan_per_packet,
        3 * chan_per_packet,
        2 * chan_per_packet,
        1 * chan_per_packet,
        0 * chan_per_packet
    );
    const int scale = 4;

    unsigned isamp, ichan;

    /* New improved more cache friendly version on CPU */
    for (isamp=0; isamp<samp_per_packet/16; ++isamp)
    {
        // Init pout for this set of 16 time samples
        pout = (__m512i *)(((int *)out) + 16 * isamp);

        for (ichan=0; ichan<chan_per_packet; ++ichan)
        {
            // Load
            data = _mm512_i32gather_epi32(vindex, pin++, scale);
            // Store
            _mm512_stream_si512(pout, data);
            // Increment pout by samp_per_block/16
            pout += samp_per_block/16;
        }

        // Skip over next 15 time samples in input
        pin += 15 * chan_per_packet;
    }
}
#endif // HAVE_AVX512F_INSTRUCTIONS
#endif // 0 (inefficient)

void hpguppi_udp_packet_data_copy_transpose_from_payload(char *databuf, int nchan,
        unsigned block_pkt_idx, unsigned packets_per_block,
        const char *payload, size_t payload_size)
{
    const unsigned chan_per_packet = nchan;
    const size_t bytes_per_sample = 4;
    const unsigned samp_per_packet = hpguppi_udp_packet_datasize(payload_size)
        / bytes_per_sample / chan_per_packet;

    char *optr;

    const char *in = hpguppi_udp_packet_data_from_payload(payload, payload_size);
    optr = databuf + bytes_per_sample * (block_pkt_idx*samp_per_packet);

#ifdef TRANSPOSE_NETBLKS_ON_GPU
    // tranpose on gpu, so just copy the packet into place as is
    memcpy(optr, in, chan_per_packet*samp_per_packet*bytes_per_sample);
#else
    // transpose on CPU before loading into buffer
    const unsigned samp_per_block = packets_per_block * samp_per_packet;
    const char *iptr;
    unsigned isamp,ichan;

    // Arrange data from network packet format e.g:
    // S0C0P0123, S0C1P0123, S0C2P0123, ... S0CnP0123,
    // S1C0P0123, S1C1P0123, S1C2P0123, ... S1CnP0123,
    // S2C0P0123, S2C1P0123, S3C2P0123, ... S2CnP0123,
    // S3C0P0123, S3C1P0123, S3C2P0123, ... S3CnP0123,
    // SmCnP0123

    // Into the format:
    // S0C0P0123, S1C0P0123, S2C0P0123, ... SmC0P0123
    // S0C1P0123/ S1C1P0123, S2C1P0123, ... SmC1P0123
    // S0C2P0123/ S1C2P0123, S2C2P0123, ... SmC1P0123
    // S0C3P0123/ S1C3P0123, S2C3P0123, ... SmC1P0123
    /// ...
    // SmC0P0123

    #if 0
    /* Previous CPU based routine, cache unfriendly */
    iptr = hpguppi_udp_packet_data_from_payload(payload, payload_size);
    for (isamp=0; isamp<samp_per_packet; isamp++) {
        optr = databuf + bytes_per_sample * (block_pkt_idx*samp_per_packet
                + isamp);
        for (ichan=0; ichan<chan_per_packet; ichan++) {
            memcpy(optr, iptr, bytes_per_sample);
            iptr += bytes_per_sample;
            optr += bytes_per_sample*samp_per_block;
        }
    }
    #else
    /* New improved more cache friendly version on CPU */
    for (ichan=0; ichan<chan_per_packet; ++ichan)
    {
        iptr = in + (ichan*bytes_per_sample);
        for (isamp=0; isamp<samp_per_packet; ++isamp)
        {
            memcpy(optr, iptr, bytes_per_sample);
            optr += bytes_per_sample;
            iptr += chan_per_packet*bytes_per_sample;
        }
        optr += bytes_per_sample*(samp_per_block-samp_per_packet);
    }
    #endif
#endif
}

void hpguppi_s6_packet_data_copy_from_payload(char *databuf, int block_chan,
        unsigned block_time, unsigned obsnchan,
        const char *payload, size_t payload_size)
{
    const size_t bytes_per_sample = 4; // Xr,Xi,Yr,Yi
    // -16 for S6 header and footer bytes
    const unsigned chan_per_packet = (payload_size - 16) / bytes_per_sample;
    unsigned ichan;

#if 0
    hashpipe_warn("hpguppi_s6_packet_data_copy_transpose_from_payload",
            "databuf %lp block_chan %d block_time %d ntime_per_block %d payload_size %d",
            databuf, block_chan, block_time, ntime_per_block, payload_size);
#endif // 0

#ifndef DEBUG_S6_COPY
    const uint64_t *iptr = (const uint64_t *)(payload + 8);
#endif // DEBUG_S6_COPY

    uint64_t *optr = (uint64_t *)(databuf
                   + block_time * obsnchan * bytes_per_sample
                   + block_chan * bytes_per_sample);

    // Arrange data from network packet format e.g:
    //
    // X0r|X0i|X1r|X1i|Y0r|Y0i|Y1r|Y1i  == [SsC0P0, SsC1P0, SsC0P1, SsC1P1]
    // X2r|X2i|X3r|X3i|Y2r|Y2i|Y3r|Y3i  == [SsC2P0, SsC3P0, SsC2P1, SsC3P1]
    // ...

    // Into the format:
    //
    // S0C0P01, S0C1P01, S0C2P01, ... S0CnP01
    // S1C0P01, S1C1P01, S1C2P01, ... S1CnP01
    // S2C0P01, S2C1P01, S2C2P01, ... S2CnP01
    // S3C0P01, S3C1P01, S3C2P01, ... S3CnP01
    /// ...

#if 0
    memcpy(optr, iptr, payload_size - 16);
#else
    // Unpack the unusual X0 X1 Y0 Y1 order into the normal X0 Y0 X1 Y1 order.
    // This code is specific to little endian systems.  To make it more general,
    // should use be64toh and htobe64.
    for (ichan=0; ichan<chan_per_packet/2; ++ichan)
    {
        // In mem: 00 11 22 33 44 55 66 77
        // In reg: 77 66 55 44 33 22 11 00
        uint64_t d = *iptr++;

        // In reg: 77 66 33 22 55 44 11 00
        // In mem: 00 11 44 55 22 33 66 77
        *optr++ = ((d    ) & 0xffff00000000ffff)
                | ((d<<16) & 0x0000ffff00000000)
                | ((d>>16) & 0x00000000ffff0000);
    }
#endif
}

void hpguppi_s6_packet_data_copy_transpose_from_payload(char *databuf, int block_chan,
        unsigned block_time, unsigned ntime_per_block,
        const char *payload, size_t payload_size)
{
    const size_t bytes_per_sample = 4; // Xr,Xi,Yr,Yi
    // -16 for S6 header and footer bytes
    const unsigned chan_per_packet = (payload_size - 16) / bytes_per_sample;

#if 0
    hashpipe_warn("hpguppi_s6_packet_data_copy_from_payload",
            "databuf %lp block_chan %d block_time %d ntime_per_block %d payload_size %d",
            databuf, block_chan, block_time, ntime_per_block, payload_size);
#endif // 0

#ifndef DEBUG_S6_COPY
    const uint64_t *iptr = (const uint64_t *)(payload + 8);
#endif // DEBUG_S6_COPY

    uint32_t *optr = (uint32_t *)(databuf
                   + block_chan * ntime_per_block * bytes_per_sample
                   + block_time * bytes_per_sample);

    unsigned ichan;

    // Arrange data from network packet format e.g:
    // X0r|X0i|X1r|X1i|Y0r|Y0i|Y1r|Y1i
    // X2r|X2i|X3r|X3i|Y2r|Y2i|Y3r|Y3i
    // ...

    // Into the format:
    // S0C0P01, S1C0P01, S2C0P01, ... SmC0P01
    // S0C1P01, S1C1P01, S2C1P01, ... SmC1P01
    // S0C2P01, S1C2P01, S2C2P01, ... SmC1P01
    // S0C3P01, S1C3P01, S2C3P01, ... SmC1P01
    /// ...

    for (ichan=0; ichan<chan_per_packet/2; ++ichan)
    {
#ifndef DEBUG_S6_COPY
        uint64_t d = *iptr++;
        *optr = ((d>>32) & 0xffff0000)
              | ((d>>16) & 0x0000ffff);
#else
        // Store lower 16 bits of block time and channel number packed as two
        // big endian 16 bit values.  This is intended to facilitate debugging
        // via dumping the data buffers.
        *optr = htonl(
                  ((block_time & 0xffff) << 16) | (block_chan + 2*ichan)
                );
#endif // DEBUG_S6_COPY
        optr += (ntime_per_block * bytes_per_sample) / sizeof(uint32_t);
#ifndef DEBUG_S6_COPY
        *optr = ((d>>16) & 0xffff0000)
              | ((d    ) & 0x0000ffff);
#else
        // Store lower 16 bits of block time and channel number packed as two
        // big endian 16 bit values.  This is intended to facilitate debugging
        // via dumping the data buffers.
        *optr = htonl(
                  ((block_time & 0xffff) << 16) | (block_chan + 2*ichan + 1)
                );
#endif // DEBUG_S6_COPY
        optr += (ntime_per_block * bytes_per_sample) / sizeof(uint32_t);
    }
}

void hpguppi_s6mb_packet_data_copy_from_payload(char *databuf, int block_chan,
        unsigned block_time, unsigned ntime_per_block,
        const char *payload, size_t payload_size)
{
    const size_t bytes_per_sample = 4; // Xr,Xi,Yr,Yi
    const unsigned chan_per_packet = 4;
    const unsigned time_per_packet = 512;
    unsigned ichan;

    const char *iptr = (payload + 8);
    const uint64_t istride = time_per_packet * bytes_per_sample;

    char *optr = (databuf
                   + block_chan * ntime_per_block * bytes_per_sample
                   + block_time * bytes_per_sample);
    const uint64_t ostride = ntime_per_block * bytes_per_sample;

    // Copy data from network packet format e.g:
    //
    // T000X0r|T000X0i|T000Y0r|T000Y0i|T001X0r|T001X0i|T001Y0r|T001Y0i
    // T002X0r|T002X0i|T002Y0r|T002Y0i|T003X0r|T003X0i|T003Y0r|T003Y0i
    // ...
    // T510X0r|T510X0i|T510Y0r|T510Y0i|T511X0r|T511X0i|T511Y0r|T511Y0i
    // T000X1r|T000X1i|T000Y1r|T000Y1i|T001X1r|T001X1i|T001Y1r|T001Y1i
    // T002X1r|T002X1i|T002Y1r|T002Y1i|T003X1r|T003X1i|T003Y1r|T003Y1i
    // ...
    // T510X1r|T510X1i|T510Y1r|T510Y1i|T511X1r|T511X1i|T511Y1r|T511Y1i
    // T000X2r|T000X2i|T000Y2r|T000Y2i|T001X2r|T001X2i|T001Y2r|T001Y2i
    // T002X2r|T002X2i|T002Y2r|T002Y2i|T003X2r|T003X2i|T003Y2r|T003Y2i
    // ...
    // T510X2r|T510X2i|T510Y2r|T510Y2i|T511X2r|T511X2i|T511Y2r|T511Y2i
    // T000X3r|T000X3i|T000Y3r|T000Y3i|T001X3r|T001X3i|T001Y3r|T001Y3i
    // T002X3r|T002X3i|T002Y3r|T002Y3i|T003X3r|T003X3i|T003Y3r|T003Y3i
    // ...
    // T510X3r|T510X3i|T510Y3r|T510Y3i|T511X3r|T511X3i|T511Y3r|T511Y3i

    // Unpack the unusual X0 X1 Y0 Y1 order into the normal X0 Y0 X1 Y1 order.
    // This code is specific to little endian systems.  To make it more general,
    // should use be64toh and htobe64.
    for (ichan=0; ichan<chan_per_packet; ++ichan)
    {
      memcpy(optr, iptr, istride);
      iptr += istride;
      optr += ostride;
    }
}

size_t parkes_udp_packet_datasize(size_t packet_size) {
    return(packet_size - sizeof(unsigned long long));
}

void parkes_to_guppi(struct hpguppi_udp_packet *b, const int acc_len, 
        const int npol, const int nchan)
{
    parkes_to_guppi_from_payload(b->data, acc_len, npol, nchan);
}

void parkes_to_guppi_from_payload(char *payload, const int acc_len, 
        const int npol, const int nchan)
{

    /* Convert IBOB clock count to packet count.
     * This assumes 2 samples per IBOB clock, and that
     * acc_len is the actual accumulation length (=reg_acclen+1).
     */
    const unsigned int counts_per_packet = (nchan/2) * acc_len;
    unsigned long long *packet_idx = (unsigned long long *)payload;
    (*packet_idx) = change_endian64(packet_idx);
    (*packet_idx) /= counts_per_packet;
    (*packet_idx) = change_endian64(packet_idx);

    /* Reorder from the 2-pol Parkes ordering */
    int i;
    char tmp[GUPPI_MAX_PACKET_SIZE];
    char *pol0, *pol1, *pol2, *pol3, *in;
    in = payload + sizeof(long long);
    if (npol==2) {
        pol0 = &tmp[0];
        pol1 = &tmp[nchan];
        for (i=0; i<nchan/2; i++) {
            /* Each loop handles 2 values from each pol */
            memcpy(pol0, in, 2*sizeof(char));
            memcpy(pol1, &in[2], 2*sizeof(char));
            pol0 += 2;
            pol1 += 2;
            in += 4;
        }
    } else if (npol==4) {
        pol0 = &tmp[0];
        pol1 = &tmp[nchan];
        pol2 = &tmp[2*nchan];
        pol3 = &tmp[3*nchan];
        for (i=0; i<nchan; i++) {
            /* Each loop handles one sample */
            *pol0 = *in; in++; pol0++;
            *pol1 = *in; in++; pol1++;
            *pol2 = *in; in++; pol2++;
            *pol3 = *in; in++; pol3++;
        }
    }
    memcpy(payload + sizeof(long long), tmp, sizeof(char) * npol * nchan);
}

int hpguppi_udp_close(struct hpguppi_udp_params *p) {
    close(p->sock);
    return(HASHPIPE_OK);
}
