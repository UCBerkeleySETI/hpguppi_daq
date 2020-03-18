// # hpguppi_pksuwl.h - Definitions for Parkes UWL receiver packets
//
// ## The Parkes UWL Receiver
//
// The Parkes Ultra-Wideband Low recevier digitizes the signal within the
// receiver itself.  These data are then channelized into 128 MHz wide
// critically sampled sub-bands and sent out via multicast UDP packets
// containing VDIF formatted data.  Each packet contains a single VDIF data
// frame consisting of a VDIF data header and a VDIF data array.  The VDIF data
// array contains 2048 time samples of a single polarization of a single 128
// MHz sub-band.  Each sample is a 16 bit real + 16 bit imaginary complex
// voltage.
//
// Because the samples are complex, the sampling period for a sub-band is
// 1/128e6 seconds per sample (i.e. 7.8125 ns/sample).  Each packet therefore
// spans 2048/128e6 seconds (i.e. 16 usec/packet).  The number of packets per
// second per polarization is 128e6/2048 (i.e. 62500 packets per second per
// polarization).
//
// ## GUPPI RAW Blocks and Shared Memory Blocks
//
// The GUPPI RAW format stores data in fixed sized blocks.  The hpguppi network
// threads for other receivers use a shared memory block size of 128 MiB (i.e.
// 128*1024*1024).  To facilitate compatibility with these other network
// threads, the hpguppi_pksuwl_net_thread also uses a shared memory block size
// of 128 MiB, but it creates GUPPI RAW blocks that are that size or smaller.
//
// The GUPPI RAW block size must also be an integer multiple of the packet size
// to avoid compilcations arising from splitting a packet across block
// boundaries.
//
// Another constraint on block size is that the real-time spectroscopy code
// (i.e. rawspec) only works on an integer number of blocks.  This means that
// the highest spectral resolution desired must use an integer multiple of
// blocks.  The spectroscopy code also places constraints on the resolution of
// other (lower resolution) products computed at the same time as well as their
// integration times, but those constraints do not affect the block sizing.
//
// ## Block Size and Spectral Resolution
//
// Given that the highest desired spectral resoultion is ~1 Hz per channel,
// some possible spectral resolutions for a 128 MHz sub-band are:
//
//     128e6 Hz / 2**27 channels == 0.954 Hz/channel, 1.049 sec/spectrum
//     128e6 Hz / 128e6 channels == 1.000 Hz/channel, 1.000 sec/spectrum
//     128e6 Hz / 2**26 channels == 1.907 Hz/channel, 0.524 sec/spectrum
//     128e6 Hz /  64e6 channels == 2.000 Hz/channel, 0.500 sec/spectrum
//
// The 2**N channel options are appealing in terms of FFT efficiency and
// simplicity, but the spectrum would rarely align with one second boundaries.
// The 2**N * 1e6 channel options provide "cleaner" alignment with one second
// boundaries, but are not as simple in terms of FFTs.  Fortunately, modern FFT
// libraries (e.g. FFTW and CuFFT) are well suited for handling FFT lengths
// that are a product of powers of small primes (2 and 5 in the cases presented
// here).
//
// For the 2**N channel options, the shared memory block size of 128 MiB ==
// 2**27 will fit 2**13 == 8192 packets from each polarization:
//
//     2**27 bytes = 2**13 bytes/packet * 2**13 packets/pol * 2 pols
//
// This works out to 2**24 samples per block per pol, so 2**27 channels would
// require 2**3 blocks and 2**26 channels would require 2**2 blocks.
//
// For the 2**N * 1e6 channel options, we find that 5**5 * 2 == 6250 packets
// per polarization span 0.1 seconds and both polarizations would occupy a
// total of 102,400,000 bytes (clearly less than 128 MiB):
//
//     2**10 * 1e5 bytes == 2**13 bytes/pkt * (5**5 * 2) pkts/pol * 2 pols
//
// For a 0.1 second block duration, it can be seen that 2 Hz resolution would
// require 5 blocks (0.5 seconds) and 1 Hz resolution would require 10 blocks
// (1 second).
//
// ## VDIF to GUPPI RAW Conversion
//
// ### Polarization handling
//
// The VDIF formatted data from each polarization arrive in separate packets,
// but the GUPPI RAW format (and rawspec) requires that the two polarizations
// be interleaved.  This requires some extra data manipulation when placing
// packet data in the GUPPI RAW shared memory blocks.
//
// ### Integer representation
//
// VDIF formatted data represents integer values in offset binary form.  GUPPI
// RAW represents integer values using two's complement form.  The VDIF data
// must be converted from offset binary representation to the two's complement
// representation used by GUPPI RAW.  This can be performed by simply inverting
// the most significant bit of each interger value.
//
// VDIF specifies that multi-byte values, such as the 16 bit samples from the
// Parkes UWL receiver, be passed in little endian format (i.e. least
// significant byte first).  It is not clear which endianess is used by the
// RAW format, but for now we will optimistically assume that it is also
// little-endian.
//
// ## Time representation
//
// VDIF and GUPPI RAW also differ in how they track time.  VDIF uses three
// fields: a reference epoch, the number of seconds since the reference epoch,
// and the data frame (i.e. packet) sequence number within the second.  GUPPI
// RAW uses a number of independent fields to represent time in different ways,
// but the most precise one is PXTIDX (packet index).  PKTIDX is a
// monotonically increasing counter that has a direct relationship to elapsed
// time since the counter was last reset.  By knowing this relationship and the
// time at which the counter was reset, the absolute time corresponding to a
// given PKTIDX can be calculated.  In practice, conversion to absolute time is
// rarely performed.  More often, absolute times (scan start, scan stop, etc)
// are converted into PKTIDX values.  PKTIDX is used to reassemble the packets
// into a continuous sequence of data and to control the start/stop of
// recording based on absolute times that have been converted to values in the
// same timebase as PKTIDX.
//
// The Parkes UWL recevier has an integer number of packet per second per
// polarization, so a PKTIDX counter can be synthesized from the VDIF time
// representation.  The second represented by the reference epoch and seconds
// since the reference epoch is converted to seconds since the UNIX epoch
// (1970-01-01 00:00:00 UTC), multiplied by packets per second per
// polarization, and then added to the packet sequence number within the
// second.
//
// ## Other Miscellaneous Comments
//
// The smallest atomic component of a GUPPI RAW block can be referred to as a
// "block unit".  The 2048 time samples of a packet define the time dimension
// of a block unit.  Each block unit will be comprised of two packets, one per
// polarization for the same 2048 sample timespan.  The two polarization
// samples for a given sample time define the second dimension of the block
// unit.  In the Parkes UWL case, with dual polarization 16-bit complex
// samples, this second dimension of a block unit is 8 bytes.  This means that
// a block unit is 16384 bytes (2048 time samples * 8 bytes per time sample).
//

#ifndef _HPGUPPI_PKSUWL_H_
#define _HPGUPPI_PKSUWL_H_

#include "hashpipe_packet.h"
#include "hpguppi_vdif.h"

#define PKSUWL_SAMPLES_PER_SEC (128*1000*1000)

// This could be derived from packet data.
// data_array_size / 2 (for 16 bit samples) / 2 for complex samples
#define PKSUWL_SAMPLES_PER_PKT (8192/2/2)

#define PKSUWL_NS_PER_PKT \
  ((1000UL*1000*1000*PKSUWL_SAMPLES_PER_PKT) / PKSUWL_SAMPLES_PER_SEC)

#define PKSUWL_PKTIDX_PER_SEC \
  (PKSUWL_SAMPLES_PER_SEC / PKSUWL_SAMPLES_PER_PKT)

// This code supports two possible PKSUWL_BLOCK_DATA_SIZE values.  One for a
// power of two number of samples per block, and one for 1e6 times a power of
// two samples per block.  The different block sizes lead to a different number
// of packets per block per polarization, referred to as
// PKSUWL_PKTIDX_PER_BLOCK.  Because PKTIDX values are shared across multiple
// polarizations, this is inherently the number of PKTIDX per block per
// polarization.  The number of packets per block is
// 2*PKSUWL_PKTIDX_PER_BLOCK/PKSUWL_PKTIDX_PER_PKT, but PKSUWL_PKTIDX_PER_PKT
// is 1 so that value is taken to be implicit and is not explicitly defined.

#ifdef USE_POWER_OF_TWO_NCHAN
// If we use a power of two number of channels, the GUPPI RAW block is the same
// as the shared memory block size and the number of PKTIDX values per block is
// 8192.
#define PKSUWL_BLOCK_DATA_SIZE (BLOCK_DATA_SIZE)
#define PKSUWL_PKTIDX_PER_BLOCK (8192)
#else
// For the 2**N * 1e6 channel options, we find that 5**5 * 2 == 6250 packets
// per polarization span 0.1 seconds and both polarizations would occupy a
// total of 102,400,000 bytes (clearly less than 128 MiB):
//
//     2**10 * 1e5 bytes == 2**13 bytes/pkt * (5**5 * 2) pkts/pol * 2 pols
#define PKSUWL_BLOCK_DATA_SIZE (1024*100*1000) // in bytes
#define PKSUWL_PKTIDX_PER_BLOCK (6250)
#endif // USE_POWER_OF_TWO_NCHAN

static inline
uint64_t
pksuwl_get_pktidx(struct vdifhdr * p)
{
  uint64_t pktidx = ((uint64_t)PKSUWL_PKTIDX_PER_SEC) * vdif_get_time(p);
  pktidx += vdif_get_data_frame_seq(p);
  return pktidx;
}

static inline
void
pksuwl_pktidx_to_timeval(uint64_t pktidx, struct timeval *tv)
{
  tv->tv_sec = pktidx / PKSUWL_PKTIDX_PER_SEC;
  tv->tv_usec = (pktidx % PKSUWL_PKTIDX_PER_SEC) * PKSUWL_NS_PER_PKT / 1000;
}

#if 0
// Not used, but here it is if needed someday...
static inline
void
pksuwl_pktidx_to_timespec(uint64_t pktidx, struct timespec *ts)
{
  ts->tv_sec = pktidx / PKSUWL_PKTIDX_PER_SEC;
  ts->tv_nsec = (pktidx % PKSUWL_PKTIDX_PER_SEC) * PKSUWL_NS_PER_PKT;
}
#endif

#endif // _HPGUPPI_PKSUWL_H_
