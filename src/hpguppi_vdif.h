// hpguppi_vdif.h - Definitions for VDIF packet headers and related functions.

#ifndef _HPGUPPI_VDIF_H_
#define _HPGUPPI_VDIF_H_

#include <stdint.h>
#include <endian.h>
#include <time.h>
#include <string.h>
#include <netinet/if_ether.h>
#include <netinet/ip.h>
#include <netinet/udp.h>

// VDIF header
struct vdifhdr {
  // The VDIF spec mandates that the words are ALWAYS little-endian
  uint32_t words[8];
} __attribute__((__packed__));

// Generate a mask of consecutive 1 bits.
// Mask is n bits wide and shifted left s bits.
// vdif_bitmask(4,2) ==> 0x3c
#define vdif_bitmask(n,s) (((1<<(n))-1) << (s))

// p is pointer to struct vdifhdr
// w is index into words field
// n is width of bitfield
// s is offset of bitfields least significant bit
#define vdif_get_field(p,w,n,s) \
  ((le32toh(p->words[w])>>(s)) & vdif_bitmask(n,0))

// p is pointer to struct vdifhdr
// w is index into words field
// n is width of bitfield
// s is offset of bitfields least significant bit
// v is value to store in field
#define vdif_set_field(p,w,n,s,v) \
  (p->words[w] = htole32( \
                 (le32toh(p->words[w]) & ~vdif_bitmask(n,s)) \
               | (((v) & vdif_bitmask(n,0)) << (s))))

// Word 0 getters

static inline
uint32_t
vdif_get_invalid(struct vdifhdr * p)
{
  return vdif_get_field(p, 0, 1, 31);
}

static inline
uint32_t
vdif_get_legacy(struct vdifhdr * p)
{
  return vdif_get_field(p, 0, 1, 30);
}

static inline
uint32_t
vdif_get_ref_epoch_secs(struct vdifhdr * p)
{
  return vdif_get_field(p, 0, 30, 0);
}

// Word 1 getters

static inline
uint32_t
vdif_get_ref_epoch(struct vdifhdr * p)
{
  return vdif_get_field(p, 1, 6, 24);
}

static inline
uint32_t
vdif_get_data_frame_seq(struct vdifhdr * p)
{
  return vdif_get_field(p, 1, 24, 0);
}

// Word 2 getters

static inline
uint32_t
vdif_get_vdif_version(struct vdifhdr * p)
{
  return vdif_get_field(p, 2, 3, 29);
}

static inline
uint32_t
vdif_get_log2_nchan(struct vdifhdr * p)
{
  return vdif_get_field(p, 2, 5, 24);
}

static inline
uint32_t
vdif_get_data_frame_length(struct vdifhdr * p)
{
  return vdif_get_field(p, 2, 24, 0) * 8;
}

static inline
uint32_t
vdif_get_data_array_length(struct vdifhdr * p)
{
  return vdif_get_data_frame_length(p) - sizeof(struct vdifhdr);
}

// Word 3 getters

static inline
uint32_t
vdif_get_complex(struct vdifhdr * p)
{
  return vdif_get_field(p, 3, 1, 31);
}

static inline
uint32_t
vdif_get_bits_per_sample(struct vdifhdr * p)
{
  return vdif_get_field(p, 3, 5, 26) + 1;
}

static inline
uint32_t
vdif_get_thread_id(struct vdifhdr * p)
{
  return vdif_get_field(p, 3, 10, 16);
}

static inline
uint32_t
vdif_get_station_id(struct vdifhdr * p)
{
  return vdif_get_field(p, 3, 16, 0);
}

// Word 4 through 8 getters

static inline
uint32_t
vdif_get_extended_data_version(struct vdifhdr * p)
{
  return vdif_get_field(p, 4, 8, 24);
}

static inline
uint32_t
vdif_get_extended_data(struct vdifhdr * p, int i)
{
  return vdif_get_field(p, i+4, (i==0 ? 24 : 32), 0);
}

static inline
void
vdif_get_ref_epoch_tm(struct vdifhdr * p, struct tm * tm)
{
  uint32_t ref_epoch = vdif_get_ref_epoch(p);
  memset(tm, 0, sizeof(struct tm));
  // tm_year = year - 1900
  tm->tm_year = 2000 + ref_epoch / 2 - 1900;
  // tm_mon is 0 for jan, 6 for july
  tm->tm_mon = 6 * (ref_epoch & 1);
  tm->tm_mday = 1;
}

static inline
time_t
vdif_get_time(struct vdifhdr * p)
{
  struct tm tm;
  time_t time;

  vdif_get_ref_epoch_tm(p, &tm);
  // Subtract timezone offset to get UTC
  time = mktime(&tm) - timezone;
  time += vdif_get_ref_epoch_secs(p);
  return time;
}

static inline
void
vdif_get_timespec(struct vdifhdr * p,
    struct timespec *ts, uint32_t ns_per_data_frame)
{
  struct tm tm;

  vdif_get_ref_epoch_tm(p, &tm);
  // Subtract timezone offset to get UTC
  ts->tv_sec = mktime(&tm) - timezone;
  ts->tv_sec += vdif_get_ref_epoch_secs(p);
  ts->tv_nsec = ns_per_data_frame * vdif_get_data_frame_seq(p);
}

static inline
struct vdifhdr *
vdif_hdr_from_eth(void * p)
{
  off_t offset = sizeof(struct ethhdr)
               + sizeof(struct iphdr)
               + sizeof(struct udphdr);
  if(ntohs(((struct ethhdr *)p)->h_proto) == ETH_P_8021Q) {
    offset += 4;
  }
  return (struct vdifhdr *)(p + offset);
}

#endif // _HPGUPPI_VDIF_H_
