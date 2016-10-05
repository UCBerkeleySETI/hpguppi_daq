/* hpguppi_net_thread.c
 *
 * Routine to read packets from network and put them
 * into shared memory blocks.
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
#define PKTSOCK_NBLOCKS (800)
#define PKTSOCK_NFRAMES (PKTSOCK_FRAMES_PER_BLOCK * PKTSOCK_NBLOCKS)

#define ELAPSED_NS(start,stop) \
  (((int64_t)stop.tv_sec-start.tv_sec)*1000*1000*1000+(stop.tv_nsec-start.tv_nsec))

// Define run states.  Currently three run states are defined: IDLE, ARMED, and
// RECORD.
//
// In the IDLE state, incoming packets are dropped.  Transitioning out of the
// IDLE state will reinitialize the current blocks.  A "start" command will
// transition the state from IDLE to ARMED.
//
// In the ARMED state, incoming packets are dropped until receiving a packet
// with a sequence number between the PKTIDX0 sequence number and PKTIDX0+64.
// PKTIDX0 is taken from the status buffer (defaulting to 0 if not present).
// Ideally, data taking will start at exactly PKTIDX0, but a range of starting
// sequence numbers is used to allow for missing up to a few packets at
// startup.  If receiving a packet with sequence number between 64 and 2**20,
// assume that we missed the first 64 packets.  Sequence numbers larger than
// that are assumed to be a continuation of the previous arming and are
// silently dropped.
//
// TODO The 2**20 limit should probably be the number of packets per data
//      block.
//
// Once a packet in the 0..63 range is received, the state transitions to
// RECORD.
//
// In the RECORD state, incoming packets are stored in the current block.  When
// the current block is "full", it is marked as filled and advanced to the next
// block.  Transitioning out of the RECORD state changes nothing (partial block
// will eventually be reinitialized).

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
    int overlap_packets;              // Overlap between blocks in packets
    int npacket;                      // Number of packets filled so far
    int ndropped;                     // Number of dropped packets so far
    uint64_t last_pkt;                // Last packet seq number written to block
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
void reset_stats(struct datablock_stats *d)
{
    d->npacket=0;
    d->ndropped=0;
    d->last_pkt=0;
}

/* Reset block params */
void reset_block(struct datablock_stats *d)
{
    d->block_idx = -1;
    d->packet_idx = 0;
    reset_stats(d);
}

/* Initialize block struct */
void init_block(struct datablock_stats *d, struct hpguppi_input_databuf *db,
        size_t packet_data_size, int packets_per_block, int overlap_packets) {
    d->db = db;
    d->packet_data_size = packet_data_size;
    d->packets_per_block = packets_per_block;
    d->overlap_packets = overlap_packets;
    reset_block(d);
}

/* Update block header info, set filled status */
void finalize_block(struct datablock_stats *d)
{
    if(d->block_idx < 0) {
        hashpipe_error(__FUNCTION__, "d->block_idx == %d", d->block_idx);
        pthread_exit(NULL);
    }
    char *header = hpguppi_databuf_header(d->db, d->block_idx);
    hputi8(header, "PKTIDX", d->packet_idx);
    hputi4(header, "PKTSIZE", d->packet_data_size);
    hputi4(header, "NPKT", d->npacket);
    hputi4(header, "NDROP", d->ndropped);
    hpguppi_input_databuf_set_filled(d->db, d->block_idx);
}

/* Push all blocks down a level, losing the first one */
void block_stack_push(struct datablock_stats *d, int nblock)
{
    int i;
    for (i=1; i<nblock; i++)
        memcpy(&d[i-1], &d[i], sizeof(struct datablock_stats));
}

/* Go to next block in set */
void increment_block(struct datablock_stats *d,
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
            % (d->packets_per_block - d->overlap_packets));
    reset_stats(d);
    // TODO: wait for block free here?
}

/* Check whether a certain seq num belongs in the data block */
int block_packet_check(struct datablock_stats *d,
        uint64_t seq_num)
{
    if (seq_num < d->packet_idx) return(-1);
    else if (seq_num >= d->packet_idx + d->packets_per_block) return(1);
    else return(0);
}

/* Return packet index from a pktsock frame that is assumed to contain a UDP
 * packet.
 */
uint64_t hpguppi_pktsock_seq_num(const unsigned char *p_frame)
{
    // XXX Temp for new baseband mode, blank out top 8 bits which
    // contain channel info.
    uint64_t tmp = be64toh(*(uint64_t *)PKT_UDP_DATA(p_frame));
    tmp &= 0x00FFFFFFFFFFFFFF;
    return tmp ;
}

/* Write a search mode (filterbank) style packet (from pktsock) into the
 * datablock.  Also zeroes out any dropped packets.
 */
void write_search_packet_to_block_from_pktsock_frame(
        struct datablock_stats *d, unsigned char *p_frame)
{
    if(d->block_idx < 0) {
        hashpipe_error(__FUNCTION__, "d->block_idx == %d", d->block_idx);
        return;
    }

    const uint64_t seq_num = hpguppi_pktsock_seq_num(p_frame);
    if(block_packet_check(d, seq_num) != 0) {
        hashpipe_error("hpguppi_net_thread",
                "seq_num %lld does not belong in block %d (%lld[+%d])",
                seq_num, d->block_idx, d->packet_idx, d->packets_per_block);
        return;
    }

    int64_t next_pos = seq_num - d->packet_idx;

    // Fill in from cur_pos to next_pos with zeros
    // (not robust to out of order packet arrival)
    int cur_pos=0;
    if (d->last_pkt > d->packet_idx) cur_pos = d->last_pkt - d->packet_idx + 1;
    char *dataptr = hpguppi_databuf_data(d->db, d->block_idx)
        + (cur_pos*d->packet_data_size % BLOCK_DATA_SIZE);
    for (; cur_pos<next_pos; cur_pos++) {
        memset(dataptr, 0, d->packet_data_size);
        dataptr += d->packet_data_size;
        d->npacket++;
        d->ndropped++;
    }
    hpguppi_udp_packet_data_copy_from_payload(dataptr,
        (char *)PKT_UDP_DATA(p_frame), (size_t)PKT_UDP_SIZE(p_frame));
    d->last_pkt = seq_num;
    d->npacket++;
}

/* Write a baseband mode packet into the block.  Includes a
 * corner-turn (aka transpose) of dimension nchan.
 */
void write_baseband_packet_to_block_from_pktsock_frame(
        struct datablock_stats *d, unsigned char *p_frame, int nchan)
{
    if(d->block_idx < 0) {
        hashpipe_error(__FUNCTION__, "d->block_idx == %d", d->block_idx);
        return;
    }

    const uint64_t seq_num = hpguppi_pktsock_seq_num(p_frame);
    if(block_packet_check(d, seq_num) != 0) {
        hashpipe_error("hpguppi_net_thread",
                "seq_num %lld does not belong in block %d (%lld[+%d])",
                seq_num, d->block_idx, d->packet_idx, d->packets_per_block);
        return;
    }

    int block_pkt_idx = seq_num - d->packet_idx;
    hpguppi_udp_packet_data_copy_transpose_from_payload(
            hpguppi_databuf_data(d->db, d->block_idx),
            nchan, block_pkt_idx, d->packets_per_block,
            (char *)PKT_UDP_DATA(p_frame),
            (size_t)PKT_UDP_SIZE(p_frame));

    /* Consider any skipped packets to have been dropped,
     * update counters.
     */
    if (d->last_pkt < d->packet_idx) d->last_pkt = d->packet_idx;

    //if (seq_num == d->last_pkt + 1) {
    //    d->npacket++;
    //} else {
        // Count "dropped" packets as packets and as dropped packets
        d->npacket += seq_num - d->last_pkt;
        d->ndropped += seq_num - d->last_pkt - 1;
    //}

    d->last_pkt = seq_num;
}

static int init(hashpipe_thread_args_t *args, const int fake)
{
    /* Non-network essential paramaters */
    int blocsize=BLOCK_DATA_SIZE;
    int directio=1;
    int nbits=8;
    int npol=4;
    double obsbw=187.5;
    int obsnchan=64;
    int overlap=0;
    double tbin=0.0;
    char obs_mode[80];
    char fifo_name[PATH_MAX];

    /* Create control FIFO (/tmp/hpguppi_daq_control/$inst_id) */
    int rv = mkdir(HPGUPPI_DAQ_CONTROL, 0777);
    if (rv!=0 && errno!=EEXIST) {
        hashpipe_error("hpguppi_net_thread", "Error creating control fifo directory");
        return HASHPIPE_ERR_SYS;
    } else if(errno == EEXIST) {
        errno = 0;
    }

    sprintf(fifo_name, "%s/%d", HPGUPPI_DAQ_CONTROL, args->instance_id);
    rv = mkfifo(fifo_name, 0666);
    if (rv!=0 && errno!=EEXIST) {
        hashpipe_error("hpguppi_net_thread", "Error creating control fifo");
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
    // Get network parameters (BINDHOST, BINDPORT, PKTFMT)
    hpguppi_read_pktsock_params(st.buf, p_psp);
    // Get info from status buffer if present (no change if not present)
    hgeti4(st.buf, "BLOCSIZE", &blocsize);
    hgeti4(st.buf, "DIRECTIO", &directio);
    hgeti4(st.buf, "NBITS", &nbits);
    hgeti4(st.buf, "NPOL", &npol);
    hgetr8(st.buf, "OBSBW", &obsbw);
    hgeti4(st.buf, "OBSNCHAN", &obsnchan);
    hgeti4(st.buf, "OVERLAP", &overlap);
    hgets(st.buf, "OBS_MODE", 80, obs_mode);
    // Calculate TBIN
    tbin = 2 * obsnchan / obsbw / 1e6;
    // Store bind host/port info etc in status buffer
    hputs(st.buf, "BINDHOST", p_psp->ifname);
    hputi4(st.buf, "BINDPORT", p_psp->port);
    hputs(st.buf, "PKTFMT", p_psp->packet_format);
    hputi4(st.buf, "BLOCSIZE", blocsize);
    hputi4(st.buf, "DIRECTIO", directio);
    hputi4(st.buf, "NBITS", nbits);
    hputi4(st.buf, "NPOL", npol);
    hputr8(st.buf, "OBSBW", obsbw);
    hputi4(st.buf, "OBSNCHAN", obsnchan);
    hputi4(st.buf, "OVERLAP", overlap);
    hputr8(st.buf, "TBIN", tbin);
    hputs(st.buf, "OBS_MODE", obs_mode);
    hashpipe_status_unlock_safe(&st);

    // Set up pktsock.  Make frame_size be a divisor of block size so that
    // frames will be contiguous in mapped mempory.  block_size must also be a
    // multiple of page_size.  Easiest way is to oversize the frames to be
    // 16384 bytes, which is bigger than we need, but keeps things easy.
    p_psp->ps.frame_size = PKTSOCK_BYTES_PER_FRAME;
    // total number of frames
    p_psp->ps.nframes = PKTSOCK_NFRAMES;
    // number of blocks
    p_psp->ps.nblocks = PKTSOCK_NBLOCKS;

    if(!fake) {
        rv = hashpipe_pktsock_open(&p_psp->ps, p_psp->ifname, PACKET_RX_RING);
        if (rv!=HASHPIPE_OK) {
            hashpipe_error("hpguppi_net_thread", "Error opening pktsock.");
            pthread_exit(NULL);
        }
    }

    // Store hpguppi_pktsock_params pointer in args
    args->user_data = p_psp;

    // Success!
    return 0;
}

static int init_real(hashpipe_thread_args_t *args)
{
    return init(args, 0);
}

static int init_fake(hashpipe_thread_args_t *args)
{
    return init(args, 1);
}

static void *run(hashpipe_thread_args_t * args, const int fake)
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
        hashpipe_error("hpguppi_net_thread", "Error opening control fifo)");
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
        printf("hpguppi_net_thread: Using Parkes UDP packet format.\n");
        acclen = gp.decimation_factor;
        if (acclen==0) {
            hashpipe_error("hpguppi_net_thread",
                    "ACC_LEN must be set to use Parkes format");
            pthread_exit(NULL);
        }
    }

    /* Figure out size of data in each packet, number of packets
     * per block, etc.  Changing packet size during an obs is not
     * recommended.
     */
    int block_size = BLOCK_DATA_SIZE;
    size_t packet_data_size = hpguppi_udp_packet_datasize(p_ps_params->packet_size);
    if (use_parkes_packets)
        packet_data_size = parkes_udp_packet_datasize(p_ps_params->packet_size);
    if (hgeti4(status_buf, "BLOCSIZE", &block_size)==0) {
            block_size = BLOCK_DATA_SIZE;
            hputi4(status_buf, "BLOCSIZE", block_size);
    } else {
        if (block_size > BLOCK_DATA_SIZE) {
            hashpipe_error("hpguppi_net_thread", "BLOCSIZE > databuf block_size");
            block_size = BLOCK_DATA_SIZE;
            hputi4(status_buf, "BLOCSIZE", block_size);
        }
    }
    unsigned packets_per_block = block_size / packet_data_size;

    /* If we're in baseband mode, figure out how much to overlap
     * the data blocks.
     */
    int overlap_packets=0;
    if (baseband_packets) {
        if (hgeti4(status_buf, "OVERLAP", &overlap_packets)==0) {
            overlap_packets = 0; // Default to no overlap
        } else {
            // XXX This is only true for 8-bit, 2-pol data:
            int samples_per_packet = packet_data_size / nchan / (size_t)4;
            if (overlap_packets % samples_per_packet) {
                hashpipe_error("hpguppi_net_thread",
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
    unsigned i;
    const int nblock = 2;
    struct datablock_stats blocks[nblock];
    for (i=0; i<nblock; i++)
        init_block(&blocks[i], db, packet_data_size, packets_per_block,
                overlap_packets);

    /* Convenience names for first/last blocks in set */
    struct datablock_stats *fblock, *lblock;
    fblock = &blocks[0];
    lblock = &blocks[nblock-1];

    /* Misc counters, etc */
    int rv;
    char *curdata=NULL, *curheader=NULL;
    uint64_t start_seq_num=0, seq_num, last_seq_num=2048, nextblock_seq_num=0;
    int64_t seq_num_diff;
    double drop_frac_avg=0.0;
    const double drop_lpf = 0.25;
    int netbuf_full = 0;
    char netbuf_status[128] = {};
    unsigned force_new_block=0, waiting=-1;
    enum run_states state = IDLE;

    // Heartbeat variables
    time_t lasttime = 0;
    time_t curtime = 0;
    char timestr[32] = {0};

    // Variables for fake packets
    uint64_t fake_pktidx=0, fake_pktidx_zero=0;
    const uint32_t fake_bits_per_packet = 8 * 8192;
    const uint32_t fake_packets_per_burst = 6 * 3;
    const uint32_t fake_gbps = 6;
    struct timespec fake_past_time, fake_now_time, fake_sleep_time;
    const uint64_t fake_ns_per_burst = fake_bits_per_packet * fake_packets_per_burst / fake_gbps;
    uint64_t fake_elapsed_ns;

    // Drop all packets to date
    unsigned char *p_frame;
    if(!fake) {
        while((p_frame=hashpipe_pktsock_recv_frame_nonblock(&p_ps_params->ps))) {
            hashpipe_pktsock_release_frame(p_frame);
        }
    } else {
        // Allocate p_frame and ininitialize packet
        p_frame = malloc(p_ps_params->ps.frame_size);
        // Set packet length
        PKT_NET(p_frame)[0x18] = ((p_ps_params->packet_size+8) >> 8) & 0xff;
        PKT_NET(p_frame)[0x19] = ((p_ps_params->packet_size+8)     ) & 0xff;
        // Init payload
        uint64_t *payload = (uint64_t *)PKT_UDP_DATA(p_frame);
        payload[0] = htobe64(fake_pktidx);
        for(i=1; i<1025; i++) {
            payload[i] = htobe64(0xcafefacecafeface);
        }
        // Initialize time tracking
        clock_gettime(CLOCK_MONOTONIC, &fake_past_time);
        fake_sleep_time.tv_sec = 0;
    }

    /* Main loop */
    while (run_threads()) {

        /* Wait for data */
        do {
            if(!fake) {
                p_frame = hashpipe_pktsock_recv_udp_frame(
                    &p_ps_params->ps, p_ps_params->port, 1000); // 1 second timeout
            } else {
                // Sleep after every burts, if needed, to keep data rate reasonable
                if(fake_pktidx % fake_packets_per_burst == 0) {
                    // Calculate sleep time and sleep
                    clock_gettime(CLOCK_MONOTONIC, &fake_now_time);
                    fake_elapsed_ns = ELAPSED_NS(fake_past_time, fake_now_time);
                    if(fake_elapsed_ns < fake_ns_per_burst) {
                        fake_sleep_time.tv_nsec = fake_ns_per_burst - fake_elapsed_ns;
                        nanosleep(&fake_sleep_time, NULL);
                    }
                    fake_past_time = fake_now_time;
                }

                // Reset or increment packet index.  Setting PKTIDX to -1 in
                // the status buffer (via external means) will simulate a
                // re-arming of the packet generator (i.e. ROACH2).
                hgetu8(st.buf, "PKTIDX", (unsigned long long*)&fake_pktidx_zero);
                if(fake_pktidx_zero == -1) {
                    fake_pktidx = 0;
                } else {
                    fake_pktidx++;
                }

                *(uint64_t *)PKT_UDP_DATA(p_frame) = htobe64(fake_pktidx & ((1UL<<56)-1));
            }

            // Heartbeat update?
            time(&curtime);
            if(curtime != lasttime) {
                lasttime = curtime;
                ctime_r(&curtime, timestr);
                timestr[strlen(timestr)-1] = '\0'; // Chop off trailing newline
                hashpipe_status_lock_safe(&st);
                hputs(st.buf, "DAQPULSE", timestr);
                hputs(st.buf, "DAQSTATE", state == IDLE  ? "stopped" :
                                          state == ARMED ? "armed"   : "running");
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
                hashpipe_error("hpguppi_net_thread", "error reading control fifo)");
            } else if(rv > 0) {
                // Trim newline from command, if any
                char *newline = strchr(fifo_cmd, '\n');
                if (newline!=NULL) *newline='\0';

                // Log command
                hashpipe_warn("hpguppi_net_thread", "got %s command", fifo_cmd);

                // Act on command
                if(strcasecmp(fifo_cmd, "QUIT") == 0) {
                    // Go to IDLE state
                    state = IDLE;
                    // Hashpipe will exit upon thread exit
                    pthread_exit(NULL);
                } else if(strcasecmp(fifo_cmd, "MONITOR") == 0) {
                    hashpipe_warn("hpguppi_net_thread",
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
                        // Get PKTIDX0 from status buffer if present (no change if not present)
                        hashpipe_status_lock_safe(&st);
                        hgeti8(st.buf, "PKTIDX0", (long long int *)&start_seq_num);
                        hashpipe_status_unlock_safe(&st);
                        // Ensure start_seq_num is the beginning of a block
                        start_seq_num -= start_seq_num % packets_per_block;

                        // Go to (or stay in) ARMED state
                        state = ARMED;
                    }
                } else if(strcasecmp(fifo_cmd, "STOP") == 0) {
                    // Go to IDLE state
                    state = IDLE;
                } else {
                    hashpipe_error("hpguppi_net_thread",
                            "got unrecognized command '%s'", fifo_cmd);
                }
            }

        } while (!p_frame && run_threads());

        if(!run_threads()) {
            // We're outta here!
            if(!fake) hashpipe_pktsock_release_frame(p_frame);
            break;
        }

        // If IDLE, release frame and continue main loop
        if(state == IDLE) {
            // Release frame!
            if(!fake) hashpipe_pktsock_release_frame(p_frame);
            continue;
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
            if(!fake) hashpipe_pktsock_release_frame(p_frame);
            continue;
        }

        // Get packet's sequence number
        seq_num = hpguppi_pktsock_seq_num(p_frame);

        // If ARMED, check sequence number
        if(state == ARMED) {
            if(start_seq_num <= seq_num && seq_num < start_seq_num + START_OK_MARGIN) {
                // OK, we got a startable packet!
                // Warn if seq_num is not exactly start_seq_num
                if(seq_num != start_seq_num) {
                    hashpipe_warn("hpguppi_net_thread",
                            "first packet number is not %lu (seq_num = %lu)",
                            start_seq_num, seq_num);
                }
                // Go to RECORD state
                state = RECORD;
            } else {
                // Not a startable packet!
                // Handle late starts (i.e. "low" sequence offset, but not so
                // low enough to be a startable packet) differently from
                // packets with "high" sequence offset, which are assumed to be
                // left over from a previous arming.
                if (seq_num < start_seq_num + START_LATE_MARGIN) {
                    hashpipe_warn("hpguppi_net_thread",
                            "late start detected (seq_num = %lu), returning to idle", seq_num);

                    // Go to IDLE state
                    state = IDLE;
                }

                // Release frame!
                hashpipe_pktsock_release_frame(p_frame);
                continue;
            }
        }

        // FYI: We must be in RECORD state if we get here!

        /* Update status if needed */
        if (waiting!=0) {
            hashpipe_status_lock_safe(&st);
            hputs(st.buf, status_key, "receiving");
            hashpipe_status_unlock_safe(&st);
            waiting=0;
        }

        /* Convert packet format if needed */
        if (use_parkes_packets) {
            parkes_to_guppi_from_payload(
                (char *)PKT_UDP_DATA(p_frame), acclen, npol, nchan);
        }

        /* Check seq num diff */
        seq_num_diff = seq_num - last_seq_num;
        if (seq_num_diff<=0) {
            if (seq_num_diff<-1024) {
                force_new_block=1;
            } else if (seq_num_diff==0) {
                hashpipe_warn("hpguppi_net_thread",
                        "Received duplicate packet (seq_num=%lu)",
                        seq_num);
            }
            else {
              // Release frame!
              if(!fake) hashpipe_pktsock_release_frame(p_frame);
              /* No going backwards */
              continue;
            }
        } else {
            force_new_block=0;
            if(npacket_total == 0) {
                npacket_total = 1;
                ndropped_total = 0;
            } else {
                npacket_total += seq_num_diff;
                ndropped_total += seq_num_diff - 1;
            }
        }
        last_seq_num = seq_num;

        /* Determine if we go to next block */
        if ((seq_num>=nextblock_seq_num) || force_new_block) {

            /* Update drop stats */
            if (fblock->npacket)
                drop_frac_avg = (1.0-drop_lpf)*drop_frac_avg
                    + drop_lpf *
                    (double)fblock->ndropped /
                    (double)fblock->npacket;

            hashpipe_status_lock_safe(&st);
            hputi8(st.buf, "PKTIDX", fblock->packet_idx);
            hputr8(st.buf, "DROPAVG", drop_frac_avg);
            hputr8(st.buf, "DROPTOT",
                    npacket_total ?
                    (double)ndropped_total/(double)npacket_total
                    : 0.0);
            hputr8(st.buf, "DROPBLK",
                    fblock->npacket ?
                    (double)fblock->ndropped/(double)fblock->npacket
                    : 0.0);
            hashpipe_status_unlock_safe(&st);

            /* Finalize first block, and push it off the list.
             * Then grab next available block.
             */
            if (fblock->block_idx>=0) finalize_block(fblock);
            block_stack_push(blocks, nblock);

            increment_block(lblock, seq_num);

            curdata = hpguppi_databuf_data(db, lblock->block_idx);
            curheader = hpguppi_databuf_header(db, lblock->block_idx);
            nextblock_seq_num = lblock->packet_idx
                + packets_per_block - overlap_packets;

            /* If new obs started, reset total counters, get start
             * time.  Start time is rounded to nearest integer
             * second, with warning if we're off that by more
             * than 100ms.  Any current blocks on the stack
             * are also finalized/reset */
            if (force_new_block) {
                /* Reset stats */
                npacket_total=0;
                ndropped_total=0;
                nbogus_total=0;

                /* Get obs start time */
                get_current_mjd(&stt_imjd, &stt_smjd, &stt_offs);
                if (stt_offs>0.5) { stt_smjd+=1; stt_offs-=1.0; }
                if (fabs(stt_offs)>0.1) {
                    hashpipe_warn("hpguppi_net_thread",
                            "Second fraction = %3.1f ms > +/-100 ms",
                            stt_offs*1e3);
                }
                stt_offs = 0.0;

                /* Warn if 1st packet number is not zero */
                if (seq_num!=0) {
                    hashpipe_warn("hpguppi_net_thread",
                            "First packet number is not 0 (seq_num=%lld)",
                            seq_num);
                }

                /* Flush any current buffers */
                for (i=0; i<nblock-1; i++) {
                    if (blocks[i].block_idx>=0)
                        finalize_block(&blocks[i]);
                    reset_block(&blocks[i]);
                }

            }

            /* Read/update current status shared mem */
            hashpipe_status_lock_safe(&st);
            if (stt_imjd!=0) {
                hputi4(st.buf, "STT_IMJD", stt_imjd);
                hputi4(st.buf, "STT_SMJD", stt_smjd);
                hputr8(st.buf, "STT_OFFS", stt_offs);
                hputi4(st.buf, "STTVALID", 1);
            } else {
                // Put a non-accurate start time to avoid polyco
                // errors.
                get_current_mjd(&stt_imjd, &stt_smjd, &stt_offs);
                hputi4(st.buf, "STT_IMJD", stt_imjd);
                hputi4(st.buf, "STT_SMJD", stt_smjd);
                hputi4(st.buf, "STTVALID", 0);
                // Reset to zero
                stt_imjd = 0;
                stt_smjd = 0;
            }
            memcpy(status_buf, st.buf, HASHPIPE_STATUS_TOTAL_SIZE);
            hashpipe_status_unlock_safe(&st);

            /* block size possibly changed on new obs */
            /* TODO: what about overlap...
             * Also, should this even be allowed ?
             */
            if (force_new_block) {
                if (hgeti4(status_buf, "BLOCSIZE", &block_size)==0) {
                        block_size = BLOCK_DATA_SIZE;
                } else {
                    if (block_size > BLOCK_DATA_SIZE) {
                        hashpipe_error("hpguppi_net_thread",
                                "BLOCSIZE > databuf block_size");
                        block_size = BLOCK_DATA_SIZE;
                    }
                }
                packets_per_block = block_size / packet_data_size;
            }
            hputi4(status_buf, "BLOCSIZE", block_size);

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
                    hashpipe_error("hpguppi_net_thread",
                            "error waiting for free databuf");
                    pthread_exit(NULL);
                }
            }
            hashpipe_status_lock_safe(&st);
            hputs(st.buf, status_key, "receiving");
            hashpipe_status_unlock_safe(&st);

            memcpy(curheader, status_buf, HASHPIPE_STATUS_TOTAL_SIZE);
            //if (baseband_packets) { memset(curdata, 0, block_size); }
            if (1) { memset(curdata, 0x45, block_size); }
        }

        /* Copy packet into any blocks where it belongs.
         * The "write packets" functions also update drop stats
         * for blocks, etc.
         */
        for (i=0; i<nblock; i++) {
            if ((blocks[i].block_idx>=0)
                    && (block_packet_check(&blocks[i],seq_num)==0)) {
                if (baseband_packets)
                    write_baseband_packet_to_block_from_pktsock_frame(
                        &blocks[i], p_frame, nchan);
                else
                    write_search_packet_to_block_from_pktsock_frame(
                        &blocks[i], p_frame);
            }
        }

        // Release frame back to ring buffer
        if(!fake) hashpipe_pktsock_release_frame(p_frame);

        /* Will exit if thread has been cancelled */
        pthread_testcancel();
    }

    pthread_exit(NULL);

    /* Have to close all push's */
    pthread_cleanup_pop(0); /* Closes hpguppi_free_psrfits */

    return NULL;
}

static void *run_real(hashpipe_thread_args_t *args)
{
    return run(args, 0);
}

static void *run_fake(hashpipe_thread_args_t *args)
{
    return run(args, 1);
}

static hashpipe_thread_desc_t real_net_thread = {
    name: "hpguppi_net_thread",
    skey: "NETSTAT",
    init: init_real,
    run:  run_real,
    ibuf_desc: {NULL},
    obuf_desc: {hpguppi_input_databuf_create}
};

static hashpipe_thread_desc_t fake_net_thread = {
    name: "hpguppi_fake_net_thread",
    skey: "NETSTAT",
    init: init_fake,
    run:  run_fake,
    ibuf_desc: {NULL},
    obuf_desc: {hpguppi_input_databuf_create}
};

static __attribute__((constructor)) void ctor()
{
  register_hashpipe_thread(&real_net_thread);
  register_hashpipe_thread(&fake_net_thread);
}

// vi: set ts=8 sw=4 et :
