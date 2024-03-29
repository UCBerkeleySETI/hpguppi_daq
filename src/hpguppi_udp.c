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

#include <hashpipe.h>

#include "hpguppi_databuf.h"
#include "hpguppi_udp.h"
//#ifdef USE_SSE_TRANSPOSE
//#include "sse_transpose.h"
//#endif


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
