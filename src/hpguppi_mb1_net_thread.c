/* hpguppi_mb1_net_thread.c
 *
 * Routine to read packets from network and put them
 * into shared memory blocks.  This thread expects to receive one beam only,
 * spread over 8 packets per mcount.
 */

#define _GNU_SOURCE 1
#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <unistd.h>
#include <string.h>
#include <pthread.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "hashpipe.h"

#include "hpguppi_databuf.h"
#include "hpguppi_params.h"
#include "hpguppi_udp.h"
#include "hpguppi_time.h"

#define HPGUPPI_DAQ_CONTROL "/tmp/hpguppi_daq_control"
#define MAX_CMD_LEN 1024

#define PKTSOCK_BYTES_PER_FRAME (16384)
#define PKTSOCK_FRAMES_PER_BLOCK (8)
//#define PKTSOCK_NBLOCKS (800)
#define PKTSOCK_NBLOCKS (800*12)
#define PKTSOCK_NFRAMES (PKTSOCK_FRAMES_PER_BLOCK * PKTSOCK_NBLOCKS)

#define ELAPSED_NS(start,stop) \
  (((int64_t)stop.tv_sec-start.tv_sec)*1000*1000*1000+(stop.tv_nsec-start.tv_nsec))

// Define run states.  Currently three run states are defined: IDLE, ARMED, and
// RECORD.
//
// In all states, the PKTIDX field is updated with the value from received
// packets.  Whenever the first PKTIDX of a block is received (i.e. whenever
// PKTIDX is a multiple of the number of packets per block), the value for
// PKTSTART and DWELL are read from the status buffer.  PKTSTART is rounded
// down, if needed, to ensure that it is a multiple of the number of packets
// per block, then PKTSTART is written back to the status buffer.  DWELL is
// interpreted as the number of seconds to record and is used to calculate
// PKTSTOP (which gets rounded down, if needed, to be a multiple of the number
// of packets per block).
//
// In the IDLE state, incoming packets are dropped, but PKTIDX is still
// updated.  Transitioning out of the IDLE state will reinitialize the current
// blocks.  A "start" command will transition the state from IDLE to ARMED.
// A "stop" command will transition the state from ARMED or RECORD to IDLE.
//
// In the ARMED state, incoming packets are processed (i.e. stored in the net
// thread's output buffer) and full blocks are passed to the next thread.  When
// the processed PKTIDX is equal to PKTSTART the state transitions to RECORD
// and the following actions occur:
//
//   1. The MJD of the observation start time is calculated (TODO Calculate
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
// as in the ARMED state).  When the processed PKTIDX is greater than or equal
// to PKTSTOP the state transitions to ARMED and STTVALID is set to 0.
//
// The downstream thread (i.e. hpguppi_rawdisk_thread) is expected to use a
// combination of PKTIDX, PKTSTART, PKTSTOP, and (optionally) STTVALID to
// determine whether the blocks should be discarded or processed (e.g. written
// to disk).

enum run_states {IDLE, ARMED, RECORD};

static const uint64_t START_OK_MARGIN   =      64;
static const uint64_t START_LATE_MARGIN = (1<<20);

/* It's easier to just make these global ... */
static uint64_t npacket_total=0, ndropped_total=0, nbogus_total=0;

/* Structs/functions to more easily deal with multiple
 * active blocks being filled
 */
struct datablock_stats {
    struct hpguppi_input_databuf *db; // Pointer to overall shared mem databuf
    int block_idx;                    // Block index number in databuf
    uint64_t packet_idx;              // Index of first packet number in block
    size_t packet_data_size;          // Data size of each packet
    int packets_per_block;            // Total number of packets to go in the block
    int seqnums_per_block;            // Total number of seqnums to go in the block
    int overlap_packets;              // Overlap between blocks in packets
    int npacket;                      // Number of packets filled so far
};

#if 0
// Defined in guppi_net_thread_codd.c

/* get the thread specific pid */
pid_t gettid();

/* Update block header info, set filled status */
void finalize_block(struct datablock_stats *d);

/* Push all blocks down a level, losing the first one */
void block_stack_push(struct datablock_stats *d, int nblock);

/* Go to next block in set */
void increment_block(struct datablock_stats *d,
        unsigned long long next_seq_num);

/* Check whether a certain seq num belongs in the data block */
int block_packet_check(struct datablock_stats *d,
        unsigned long long seq_num);
#endif // 0

/* Reset all counters */
static void reset_stats(struct datablock_stats *d)
{
    d->npacket=0;
}

/* Reset block params */
static void reset_block(struct datablock_stats *d)
{
    d->block_idx = -1;
    d->packet_idx = 0;
    reset_stats(d);
}

/* Initialize block struct */
static void init_block(struct datablock_stats *d, struct hpguppi_input_databuf *db,
        size_t packet_data_size, int packets_per_block, int packets_per_spectrum, int overlap_packets)
{
    d->db = db;
    d->packet_data_size = packet_data_size;
    d->packets_per_block = packets_per_block; // Eight packets will have
    d->seqnums_per_block = packets_per_block /packets_per_spectrum; // a common pktidx (aka mcount)
    d->overlap_packets = overlap_packets;
    reset_block(d);
}

/* Update block header info, set filled status */
static void finalize_block(struct datablock_stats *d)
{
    if(d->block_idx < 0) {
        hashpipe_error(__FUNCTION__, "d->block_idx == %d", d->block_idx);
        pthread_exit(NULL);
    }
    npacket_total += d->npacket;
    ndropped_total += d->packets_per_block - d->npacket;
    char *header = hpguppi_databuf_header(d->db, d->block_idx);
    char dropstat[128];
    sprintf(dropstat, "%d/%d", d->packets_per_block-d->npacket, d->packets_per_block);
    hputi8(header, "PKTIDX", d->packet_idx);
    hputi4(header, "PKTSIZE", d->packet_data_size);
    hputi4(header, "NPKT", d->npacket);
    hputi4(header, "NDROP", d->packets_per_block - d->npacket);
    hputs(header, "DROPSTAT", dropstat);
    hpguppi_input_databuf_set_filled(d->db, d->block_idx);
}

/* Push all blocks down a level, losing the first one */
static void block_stack_push(struct datablock_stats *d, int nblock)
{
    int i;
    for (i=1; i<nblock; i++)
        memcpy(&d[i-1], &d[i], sizeof(struct datablock_stats));
}

/* Go to next block in set */
static void increment_block(struct datablock_stats *d,
        uint64_t next_seq_num)
{
    if(d->block_idx < 0) {
        hashpipe_warn(__FUNCTION__, "d->block_idx == %d", d->block_idx);
    }
    if(d->db->header.n_block < 1) {
        hashpipe_error(__FUNCTION__, "d->db->header.n_block == %d", d->db->header.n_block);
        pthread_exit(NULL);
    }

    d->block_idx = (d->block_idx + 1) % d->db->header.n_block;
    d->packet_idx = next_seq_num - (next_seq_num
            % (d->seqnums_per_block - d->overlap_packets));
    reset_stats(d);
    // TODO: wait for block free here?
}

/* Check whether a certain seq num belongs in the data block */
static int block_packet_check(struct datablock_stats *d,
        uint64_t seq_num)
{
    if (seq_num < d->packet_idx) return(-1);
    else if (seq_num >= d->packet_idx + d->seqnums_per_block) return(1);
    else return(0);
}

/* Return packet index from a pktsock frame that is assumed to contain an S6
 * UDP packet.
 */
static uint64_t hpguppi_pktsock_seq_num(const unsigned char *p_frame)
{
    uint64_t tmp = be64toh(*(uint64_t *)PKT_UDP_DATA(p_frame));
    tmp >>= 16;
    return tmp ;
}

/* Return channel from a pktsock frame that is assumed to contain an S6 UDP
 * packet.
 */
static uint64_t hpguppi_pktsock_hdr_chan(const unsigned char *p_frame)
{
    uint64_t tmp = be64toh(*(uint64_t *)PKT_UDP_DATA(p_frame));
    tmp >>= 4;
    return tmp & 0xfff;
}

/* Write a baseband mode packet into the block.  Includes a
 * corner-turn (aka transpose) of dimension nchan.
 */
static void write_baseband_packet_to_block_from_pktsock_frame(
        struct datablock_stats *d, unsigned char *p_frame,
        int obsschan,
        int obsnchan,
        int ntime_per_block)
{
    if(d->block_idx < 0) {
        hashpipe_error(__FUNCTION__, "d->block_idx == %d", d->block_idx);
        return;
    }

    const uint64_t seq_num = hpguppi_pktsock_seq_num(p_frame);
    if(block_packet_check(d, seq_num) != 0) {
        hashpipe_error("hpguppi_mb1_net_thread",
                "seq_num %lld does not belong in block %d (%lld[+%d])",
                seq_num, d->block_idx, d->packet_idx, d->seqnums_per_block);
        return;
    }

    int block_chan = hpguppi_pktsock_hdr_chan(p_frame) - obsschan;
    if(block_chan < 0 || block_chan >= obsnchan) {
        // Should "never" happen, but it can if the switch gets confused
        hashpipe_warn("hpguppi_mb1_net_thread",
                "packet channel %d not in range [%d,%d)",
                hpguppi_pktsock_hdr_chan(p_frame),
                obsschan, obsschan+obsnchan);
        return;
    }

    int block_time = seq_num - d->packet_idx;
    if(block_time >= ntime_per_block) {
        // Should "never" happen (already checked above)
        block_time %= ntime_per_block;
    } else if(block_time < 0) {
        hashpipe_error("hpguppi_mb1_net_thread", "Block time less than 0! seq_num %lu d->packet_idx %lu", seq_num, d->packet_idx);
        exit(1);
    }

#if 0
    hpguppi_s6_packet_data_copy_transpose_from_payload(
            hpguppi_databuf_data(d->db, d->block_idx),
            block_chan, block_time, ntime_per_block,
            (char *)PKT_UDP_DATA(p_frame),
            (size_t)PKT_UDP_SIZE(p_frame)-8); // -8 for UDP header bytes
#else
    hpguppi_s6_packet_data_copy_from_payload(
            hpguppi_databuf_data(d->db, d->block_idx),
            block_chan, block_time, obsnchan,
            (char *)PKT_UDP_DATA(p_frame),
            (size_t)PKT_UDP_SIZE(p_frame)-8); // -8 for UDP header bytes
#endif

    d->npacket++;
}

static int init(hashpipe_thread_args_t *args)
{
    /* Non-network essential paramaters */
    int blocsize=BLOCK_DATA_SIZE;
    int directio=1;
    int nbits=8;
    int npol=4;
    double obsbw=187.5;
    int overlap=0;
    double tbin=0.0;
    char obs_mode[80];
    char fifo_name[PATH_MAX];

    /* Create control FIFO (/tmp/hpguppi_daq_control/$inst_id) */
    int rv = mkdir(HPGUPPI_DAQ_CONTROL, 0777);
    if (rv!=0 && errno!=EEXIST) {
        hashpipe_error("hpguppi_mb1_net_thread", "Error creating control fifo directory");
        return HASHPIPE_ERR_SYS;
    } else if(errno == EEXIST) {
        errno = 0;
    }

    sprintf(fifo_name, "%s/%d", HPGUPPI_DAQ_CONTROL, args->instance_id);
    rv = mkfifo(fifo_name, 0666);
    if (rv!=0 && errno!=EEXIST) {
        hashpipe_error("hpguppi_mb1_net_thread", "Error creating control fifo");
        return HASHPIPE_ERR_SYS;
    } else if(errno == EEXIST) {
        errno = 0;
    }

    struct hpguppi_pktsock_params *p_psp = (struct hpguppi_pktsock_params *)
        malloc(sizeof(struct hpguppi_pktsock_params));

    if(!p_psp) {
        perror(__FUNCTION__);
        return -1;
    }

    strcpy(obs_mode, "RAW");

    hashpipe_status_t st = args->st;

    hashpipe_status_lock_safe(&st);
    // Get network parameters (BINDHOST, BINDPORT, PKTFMT, OBSNCHAN, OBSSCHAN)
    hpguppi_read_pktsock_params(st.buf, p_psp);
    // Get info from status buffer if present (no change if not present)
    hgeti4(st.buf, "BLOCSIZE", &blocsize);
    hgeti4(st.buf, "DIRECTIO", &directio);
    hgeti4(st.buf, "NBITS", &nbits);
    hgeti4(st.buf, "NPOL", &npol);
    hgetr8(st.buf, "OBSBW", &obsbw);
    hgeti4(st.buf, "OVERLAP", &overlap);
    hgets(st.buf, "OBS_MODE", 80, obs_mode);
    // If CHPERPKT is not given, assume we get them all in 8 packets
    p_psp->chperpkt =  p_psp->obsnchan/8;
    hgeti4(st.buf, "CHPERPKT", &p_psp->chperpkt);

    // Calculate TBIN
    tbin = p_psp->obsnchan / fabs(obsbw) / 1e6;

    // Store bind host/port info etc in status buffer
    hputs(st.buf, "BINDHOST", p_psp->ifname);
    hputi4(st.buf, "BINDPORT", p_psp->port);
    hputs(st.buf, "PKTFMT", p_psp->packet_format);
    hputi4(st.buf, "BLOCSIZE", blocsize);
    hputi4(st.buf, "DIRECTIO", directio);
    hputi4(st.buf, "NBITS", nbits);
    hputi4(st.buf, "NPOL", npol);
    hputr8(st.buf, "OBSBW", obsbw);
    hputi4(st.buf, "OBSNCHAN", p_psp->obsnchan);
    hputi4(st.buf, "OBSSCHAN", p_psp->obsschan);
    hputi4(st.buf, "CHPERPKT", p_psp->chperpkt);
    hputi4(st.buf, "OVERLAP", overlap);
    // Force PKTFMT to be "S6"
    hputs(st.buf, "PKTFMT", "S6");
    hputr8(st.buf, "TBIN", tbin);
    hputs(st.buf, "OBS_MODE", obs_mode);
    // Data are in channel-major order
    // (i.e. channel dimension changes faster than time dimension)
    hputi4(st.buf, "CHANMAJ", 1);
    hashpipe_status_unlock_safe(&st);

    // Force PKTFMT to be "S6"
    strcpy(p_psp->packet_format, "S6");
    // Calculate packet size from CHPERPKT
    p_psp->packet_size = p_psp->chperpkt * 4 + 16;

    // Set up pktsock.  Make frame_size be a divisor of block size so that
    // frames will be contiguous in mapped mempory.  block_size must also be a
    // multiple of page_size.  Easiest way is to oversize the frames to be
    // 16384 bytes, which is bigger than we need, but keeps things easy.
    p_psp->ps.frame_size = PKTSOCK_BYTES_PER_FRAME;
    // total number of frames
    p_psp->ps.nframes = PKTSOCK_NFRAMES;
    // number of blocks
    p_psp->ps.nblocks = PKTSOCK_NBLOCKS;

    rv = hashpipe_pktsock_open(&p_psp->ps, p_psp->ifname, PACKET_RX_RING);
    if (rv!=HASHPIPE_OK) {
        hashpipe_error("hpguppi_mb1_net_thread", "Error opening pktsock.");
        pthread_exit(NULL);
    }

    // Store hpguppi_pktsock_params pointer in args
    args->user_data = p_psp;

    // Success!
    return 0;
}

static void *run(hashpipe_thread_args_t * args)
{
    // Local aliases to shorten access to args fields
    // Our output buffer happens to be a hpguppi_input_databuf
    hpguppi_input_databuf_t *db = (hpguppi_input_databuf_t *)args->obuf;
    hashpipe_status_t st = args->st;
    const char * status_key = args->thread_desc->skey;
    struct hpguppi_pktsock_params *p_ps_params =
        (struct hpguppi_pktsock_params *)args->user_data;

    /* Open command FIFO for read */
    char fifo_name[PATH_MAX];
    char fifo_cmd[MAX_CMD_LEN];
    sprintf(fifo_name, "%s/%d", HPGUPPI_DAQ_CONTROL, args->instance_id);
    int fifo_fd = open(fifo_name, O_RDONLY | O_NONBLOCK);
    if (fifo_fd<0) {
        hashpipe_error("hpguppi_mb1_net_thread", "Error opening control fifo)");
        pthread_exit(NULL);
    }

    /* Read in general parameters */
    struct hpguppi_params gp;
    struct psrfits pf;
    pf.sub.dat_freqs = NULL;
    pf.sub.dat_weights = NULL;
    pf.sub.dat_offsets = NULL;
    pf.sub.dat_scales = NULL;
    char status_buf[HASHPIPE_STATUS_TOTAL_SIZE];
    hashpipe_status_lock_safe(&st);
    memcpy(status_buf, st.buf, HASHPIPE_STATUS_TOTAL_SIZE);
    hashpipe_status_unlock_safe(&st);
    hpguppi_read_obs_params(status_buf, &gp, &pf);
    pthread_cleanup_push((void *)hpguppi_free_psrfits, &pf);

    /* Time parameters */
    int stt_imjd=0, stt_smjd=0;
    double stt_offs=0.0;

    /* See which packet format to use */
    int use_parkes_packets=0, baseband_packets=1;
    int nchan=0, npol=0, acclen=0;
    nchan = pf.hdr.nchan;
    npol = pf.hdr.npol;
    if (strncmp(p_ps_params->packet_format, "PARKES", 6)==0) { use_parkes_packets=1; }
    if (use_parkes_packets) {
        printf("hpguppi_mb1_net_thread: Using Parkes UDP packet format.\n");
        acclen = gp.decimation_factor;
        if (acclen==0) {
            hashpipe_error("hpguppi_mb1_net_thread",
                    "ACC_LEN must be set to use Parkes format");
            pthread_exit(NULL);
        }
    }

    /* Figure out size of data in each packet, number of packets
     * per block, etc.  Changing packet size during an obs is not
     * supported.
     */
    int block_size = BLOCK_DATA_SIZE;
    int ntime_per_block = BLOCK_DATA_SIZE / (4 * p_ps_params->obsnchan);

    // Set ntime_per_block to closest power of 2 less than or equal to
    // ntime_per_block.
    unsigned i = 0;
    while(ntime_per_block != 1) {
        ntime_per_block >>= 1;
        i++;
    }
    ntime_per_block <<= i;

    block_size = 4 * p_ps_params->obsnchan * ntime_per_block;

    if (block_size > BLOCK_DATA_SIZE) {
        hashpipe_error("hpguppi_mb1_net_thread", "BLOCSIZE > databuf block_size");
        block_size = BLOCK_DATA_SIZE;
        ntime_per_block = BLOCK_DATA_SIZE / (4 * p_ps_params->obsnchan);
    }

    // Update BLOCSIZE in status buffer
    hashpipe_status_lock_safe(&st);
    hputi4(st.buf, "BLOCSIZE", block_size);
    hashpipe_status_unlock_safe(&st);

    size_t packet_data_size = p_ps_params->packet_size - 16;
    unsigned packets_per_block = block_size / packet_data_size;
    unsigned packets_per_spectrum = p_ps_params->obsnchan/p_ps_params->chperpkt;
    unsigned seqnums_per_block = packets_per_block / packets_per_spectrum;
    hashpipe_warn("hpguppi_mb1_net_thread", "block_size %lu", block_size);
    hashpipe_warn("hpguppi_mb1_net_thread", "ntime_per_block %lu", ntime_per_block);
    hashpipe_warn("hpguppi_mb1_net_thread", "packet_data_size %lu", packet_data_size);
    hashpipe_warn("hpguppi_mb1_net_thread", "packets_per_block %u", packets_per_block);
    hashpipe_warn("hpguppi_mb1_net_thread", "seqnums_per_block %u", seqnums_per_block);

    double dwell_seconds = 300.0;
    double tbin = 1.0e6; // Dummy default value
    uint64_t dwell_blocks = 0;

    /* If we're in baseband mode, figure out how much to overlap
     * the data blocks.
     */
    int overlap_packets=0;
    if (baseband_packets) {
        if (hgeti4(status_buf, "OVERLAP", &overlap_packets)==0) {
            overlap_packets = 0; // Default to no overlap
        } else {
            // XXX This is only true for 8-bit, 2-pol data:
            int samples_per_packet = packet_data_size / (nchan/8) / (size_t)4;
            hashpipe_warn("hpguppi_mb1_net_thread", "samples_per_packet=%u", samples_per_packet);
            if (overlap_packets % samples_per_packet) {
                hashpipe_error("hpguppi_mb1_net_thread",
                        "Overlap is not an integer number of packets");
                overlap_packets = (overlap_packets/samples_per_packet+1);
                hputi4(status_buf, "OVERLAP",
                        overlap_packets*samples_per_packet);
            } else {
                overlap_packets = overlap_packets/samples_per_packet;
            }
        }
    }

    /* List of databuf blocks currently in use */
    const int nblock = 2;
    struct datablock_stats blocks[nblock];
    for (i=0; i<nblock; i++)
        init_block(&blocks[i], db, packet_data_size, packets_per_block,
                packets_per_spectrum, overlap_packets);

    /* Convenience names for first/last blocks in set */
    struct datablock_stats *fblock, *lblock;
    fblock = &blocks[0];
    lblock = &blocks[nblock-1];

    /* Misc counters, etc */
    int rv;
    char *curdata=NULL, *curheader=NULL;
    uint64_t start_seq_num=0, stop_seq_num=0, seq_num, last_seq_num=2048, nextblock_seq_num=0;
    int64_t seq_num_diff;
    double drop_frac_avg=0.0;
    const double drop_lpf = 0.25;
    int netbuf_full = 0;
    char netbuf_status[128] = {};
    unsigned force_new_block=0, waiting=-1;
    enum run_states state = IDLE;

    // Timing variables
    struct timespec ts_start, ts_stop;
    uint64_t elapsed_bytes=0, elapsed_ns=0;
    uint32_t ps_pkts=0, ps_drops=0;

    // Heartbeat variables
    time_t lasttime = 0;
    time_t curtime = 0;
    char timestr[32] = {0};

    // Drop all packets to date
    unsigned char *p_frame;
    while((p_frame=hashpipe_pktsock_recv_frame_nonblock(&p_ps_params->ps))) {
        hashpipe_pktsock_release_frame(p_frame);
    }

    /* Main loop */
    while (run_threads()) {

        /* Wait for data */
        do {
            p_frame = hashpipe_pktsock_recv_udp_frame(
                &p_ps_params->ps, p_ps_params->port, 1000); // 1 second timeout

            // Get start-of-processing time if we got a packet
            if(p_frame) {
                clock_gettime(CLOCK_MONOTONIC, &ts_start);
            }

            // Heartbeat update?
            time(&curtime);
            if(curtime != lasttime) {
                lasttime = curtime;
                ctime_r(&curtime, timestr);
                timestr[strlen(timestr)-1] = '\0'; // Chop off trailing newline
                hashpipe_status_lock_safe(&st);
                hputs(st.buf, "DAQPULSE", timestr);
                hputs(st.buf, "DAQSTATE", state == IDLE  ? "idle" :
                                          state == ARMED ? "armed"   : "record");
                hashpipe_status_unlock_safe(&st);
            }

            /* Set "waiting" flag */
            if (!p_frame && run_threads() && waiting!=1) {
                hashpipe_status_lock_safe(&st);
                hputs(st.buf, status_key, "waiting");
                hashpipe_status_unlock_safe(&st);
                waiting=1;
            }

            // Check FIFO for command
            rv = read(fifo_fd, fifo_cmd, MAX_CMD_LEN-1);
            if(rv == -1 && errno != EAGAIN) {
                hashpipe_error("hpguppi_mb1_net_thread", "error reading control fifo)");
            } else if(rv > 0) {
                // Trim newline from command, if any
                char *newline = strchr(fifo_cmd, '\n');
                if (newline!=NULL) *newline='\0';

                // Log command
                hashpipe_warn("hpguppi_mb1_net_thread", "got %s command", fifo_cmd);

                // Act on command
                if(strcasecmp(fifo_cmd, "QUIT") == 0) {
                    // Go to IDLE state
                    state = IDLE;
                    // Hashpipe will exit upon thread exit
                    pthread_exit(NULL);
                } else if(strcasecmp(fifo_cmd, "MONITOR") == 0) {
                    hashpipe_warn("hpguppi_mb1_net_thread",
                            "MONITOR command not supported, use null_output_thread.");
                } else if(strcasecmp(fifo_cmd, "START") == 0) {
                    // If in the IDLE or ARMED states
                    // (START in RECORD state is a no-op)
                    if(state == IDLE || state == ARMED) {
                        // Reset current blocks' packet_idx values and stats
                        for (i=0; i<nblock; i++) {
                            blocks[i].packet_idx = 0;
                            reset_stats(&blocks[i]);
                        }

                        // Go to (or stay in) ARMED state
                        state = ARMED;
                    }
                } else if(strcasecmp(fifo_cmd, "STOP") == 0) {
                    // Go to IDLE state
                    state = IDLE;
                } else {
                    hashpipe_error("hpguppi_mb1_net_thread",
                            "got unrecognized command '%s'", fifo_cmd);
                }
            }

        } while (!p_frame && run_threads());

        if(!run_threads()) {
            // We're outta here!
            hashpipe_pktsock_release_frame(p_frame);
            break;
        }

        /* Check packet size */
        if(p_ps_params->packet_size == 0) {
            p_ps_params->packet_size = PKT_UDP_SIZE(p_frame) - 8;
        } else if(p_ps_params->packet_size != PKT_UDP_SIZE(p_frame) - 8) {
            /* Unexpected packet size, ignore? */
            nbogus_total++;
            if(nbogus_total % 1000000 == 0) {
                hashpipe_status_lock_safe(&st);
                hputi4(st.buf, "NBOGUS", nbogus_total);
                hputi4(st.buf, "PKTSIZE", PKT_UDP_SIZE(p_frame)-8);
                hashpipe_status_unlock_safe(&st);
            }
            // Release frame!
            hashpipe_pktsock_release_frame(p_frame);
            continue;
        }

        // Get packet's sequence number
        seq_num = hpguppi_pktsock_seq_num(p_frame);

        // Update PKTIDX in status buffer if seq_num % seqnums_per_block == 0
        // and the header's channel number is the same as obsschan.
        // Also read PKTSTART, DWELL to calculate start/stop seq numbers.
        if(seq_num % seqnums_per_block == 0
        && hpguppi_pktsock_hdr_chan(p_frame) == p_ps_params->obsschan) {
            // Get packet stats
            hashpipe_pktsock_stats(&p_ps_params->ps, &ps_pkts, &ps_drops);
            hashpipe_status_lock_safe(&st);
            hputi8(st.buf, "PKTIDX", seq_num);
            hgetu8(st.buf, "PKTSTART", &start_seq_num);
            start_seq_num -= start_seq_num % seqnums_per_block;
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
            dwell_blocks = trunc(dwell_seconds / (tbin * ntime_per_block));

            stop_seq_num = start_seq_num + seqnums_per_block * dwell_blocks;
            hputi8(st.buf, "PKTSTOP", stop_seq_num);

            // Store stats
            hputi4(st.buf, "PSPKTS", ps_pkts);
            hputi4(st.buf, "PSDRPS", ps_drops);
            
            // Calculate processing speed in Gbps (8*bytes/ns)
            hputi8(st.buf, "ELPSBITS", elapsed_bytes << 3);
            hputi8(st.buf, "ELPSNS", elapsed_ns);
            hputr4(st.buf, "NETGBPS", 8.0*elapsed_bytes / elapsed_ns);
            elapsed_bytes = 0;
            elapsed_ns = 0;

            hashpipe_status_unlock_safe(&st);
        }

        /* Update status if needed */
        if (waiting!=0) {
            hashpipe_status_lock_safe(&st);
            hputs(st.buf, status_key, "receiving");
            hashpipe_status_unlock_safe(&st);
            waiting=0;
        }

        // If IDLE, release frame and continue main loop
        if(state == IDLE) {
            // Release frame!
            hashpipe_pktsock_release_frame(p_frame);
            continue;
        }

        /* Convert packet format if needed */
        if (use_parkes_packets) {
            parkes_to_guppi_from_payload(
                (char *)PKT_UDP_DATA(p_frame), acclen, npol, nchan);
        }

        /* Check seq num diff */
        seq_num_diff = seq_num - last_seq_num;
        if (seq_num_diff<0) {
            if (seq_num_diff<-1024) {
                force_new_block=1;
            } else if (seq_num_diff==0) {
                hashpipe_warn("hpguppi_mb1_net_thread",
                        "Received duplicate packet (seq_num=%lu)",
                        seq_num);
            }
            else {
              // Release frame!
              hashpipe_pktsock_release_frame(p_frame);
              /* No going backwards */
              continue;
            }
        } else {
            force_new_block=0;
        }
        last_seq_num = seq_num;

        /* Determine if we go to next block */
        if ((seq_num>=nextblock_seq_num) || force_new_block) {

            /* Update drop stats */
            if (fblock->npacket)
                drop_frac_avg = (1.0-drop_lpf)*drop_frac_avg
                    + drop_lpf *
                    (double)(fblock->packets_per_block-fblock->npacket) /
                    (double)fblock->packets_per_block;

            sprintf(netbuf_status, "%d/%d", fblock->packets_per_block-fblock->npacket, fblock->packets_per_block);
            hashpipe_status_lock_safe(&st);
            hputi8(st.buf, "PKTIDX", fblock->packet_idx);
            hputr8(st.buf, "DROPAVG", drop_frac_avg);
            hputr8(st.buf, "DROPTOT",
                    (ndropped_total+npacket_total) ?
                    (double)ndropped_total/(double)(ndropped_total+npacket_total)
                    : 0.0);
            hputr8(st.buf, "DROPBLK",
                    fblock->packets_per_block ?
                    (double)(fblock->packets_per_block-fblock->npacket)/(double)fblock->packets_per_block
                    : 0.0);
            hputs(st.buf, "DROPSTAT", netbuf_status);
            hashpipe_status_unlock_safe(&st);

            if(state == ARMED && seq_num == start_seq_num) {
                state = RECORD;

                /* Get obs start time */
                get_current_mjd(&stt_imjd, &stt_smjd, &stt_offs);
                if (stt_offs>0.5) { stt_smjd+=1; stt_offs-=1.0; }
                if (fabs(stt_offs)>0.1) {
                    hashpipe_warn("hpguppi_mb1_net_thread",
                            "Second fraction = %3.1f ms > +/-100 ms",
                            stt_offs*1e3);
                }
                stt_offs = 0.0;

                /* Reset stats */
                npacket_total=0;
                ndropped_total=0;
                nbogus_total=0;

                // Update status buffer
                hashpipe_status_lock_safe(&st);
                hputs(st.buf, "DAQSTATE", "record");
                hputi4(st.buf, "STT_IMJD", stt_imjd);
                hputi4(st.buf, "STT_SMJD", stt_smjd);
                hputr8(st.buf, "STT_OFFS", stt_offs);
                hputi4(st.buf, "STTVALID", 1);
                hashpipe_status_unlock_safe(&st);
            }

            if(state == RECORD && seq_num >= stop_seq_num) {
                state = ARMED;
                hashpipe_status_lock_safe(&st);
                hputs(st.buf, "DAQSTATE", "armed");
                hputi4(st.buf, "STTVALID", 0);
                hashpipe_status_unlock_safe(&st);
            }

            /* Finalize first block, and push it off the list.
             * Then grab next available block.
             */
            if (fblock->block_idx>=0) finalize_block(fblock);
            block_stack_push(blocks, nblock);

            increment_block(lblock, seq_num);

            curdata = hpguppi_databuf_data(db, lblock->block_idx);
            curheader = hpguppi_databuf_header(db, lblock->block_idx);
            nextblock_seq_num = lblock->packet_idx
                + seqnums_per_block - overlap_packets; // Should be overlap_seqnums!

            // If new block is forced, any current blocks on the stack are
            // finalized/reset. [TODO: Is this really needed?]
            if (force_new_block) {
                /* Flush any current buffers */
                for (i=0; i<nblock-1; i++) {
                    if (blocks[i].block_idx>=0)
                        finalize_block(&blocks[i]);
                    reset_block(&blocks[i]);
                }
            }

            /* Read/update current status shared mem */
            hashpipe_status_lock_safe(&st);
            memcpy(status_buf, st.buf, HASHPIPE_STATUS_TOTAL_SIZE);
            hashpipe_status_unlock_safe(&st);

            /* Wait for new block to be free, then clear it
             * if necessary and fill its header with new values.
             */
            netbuf_full = hpguppi_input_databuf_total_status(db);
            sprintf(netbuf_status, "%d/%d", netbuf_full, db->header.n_block);
            hashpipe_status_lock_safe(&st);
            hputs(st.buf, status_key, "waitfree");
            hputs(st.buf, "NETBUFST", netbuf_status);
            hashpipe_status_unlock_safe(&st);
            while ((rv=hpguppi_input_databuf_wait_free(db, lblock->block_idx))
                    != HASHPIPE_OK) {
                if (rv==HASHPIPE_TIMEOUT) {
                    waiting=1;
                    netbuf_full = hpguppi_input_databuf_total_status(db);
                    sprintf(netbuf_status, "%d/%d", netbuf_full, db->header.n_block);
                    hashpipe_status_lock_safe(&st);
                    hputs(st.buf, status_key, "blocked");
                    hputs(st.buf, "NETBUFST", netbuf_status);
                    hashpipe_status_unlock_safe(&st);
                    continue;
                } else {
                    hashpipe_error("hpguppi_mb1_net_thread",
                            "error waiting for free databuf");
                    pthread_exit(NULL);
                }
            }
            hashpipe_status_lock_safe(&st);
            hputs(st.buf, status_key, "receiving");
            hashpipe_status_unlock_safe(&st);

            memcpy(curheader, status_buf, HASHPIPE_STATUS_TOTAL_SIZE);
            //if (baseband_packets) { memset(curdata, 0, block_size); }
            if (1) { memset(curdata, 0x00, block_size); }
        }

        /* Copy packet into any blocks where it belongs.
         * The "write packet" function also update drop stats
         * for blocks, etc.
         */
        for (i=0; i<nblock; i++) {
            if ((blocks[i].block_idx>=0)
            && (block_packet_check(&blocks[i],seq_num)==0)) {
                write_baseband_packet_to_block_from_pktsock_frame(
                    &blocks[i], p_frame,
                    p_ps_params->obsschan,
                    p_ps_params->obsnchan,
                    ntime_per_block);
            }
        }

        // Release frame back to ring buffer
        hashpipe_pktsock_release_frame(p_frame);

        clock_gettime(CLOCK_MONOTONIC, &ts_stop);

        // -16 for S6 header and footer
        elapsed_bytes += p_ps_params->packet_size - 16;
        elapsed_ns += ELAPSED_NS(ts_start, ts_stop);

        /* Will exit if thread has been cancelled */
        pthread_testcancel();
    }

    pthread_exit(NULL);

    /* Have to close all push's */
    pthread_cleanup_pop(0); /* Closes hpguppi_free_psrfits */

    return NULL;
}

static hashpipe_thread_desc_t mb1_net_thread = {
    name: "hpguppi_mb1_net_thread",
    skey: "NETSTAT",
    init: init,
    run:  run,
    ibuf_desc: {NULL},
    obuf_desc: {hpguppi_input_databuf_create}
};

static __attribute__((constructor)) void ctor()
{
  register_hashpipe_thread(&mb1_net_thread);
}

// vi: set ts=8 sw=4 et :
