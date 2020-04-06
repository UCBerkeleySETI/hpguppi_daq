// hpguppi_util.c
//
// Utility functions for hpguppi_daq

#ifndef _HPGUPPI_UTIL_H_
#define _HPGUPPI_UTIL_H_

#include "config.h"

// Makes directory given in pathname.  Makes any intervening directories as
// needed.  Newly created directories will get permissions specified by mode.
int mkdir_p(char *pathname, mode_t mode);

#if HAVE_AVX512F_INSTRUCTIONS || HAVE_AVX2_INSTRUCTIONS

// Cache bypass (non-temporal) version of memset(dst, 0,len)
void bzero_nt(void * dst, size_t len);

// Cache bypass (non-temporal) version of memcpy(dst, sec, len)
void memcpy_nt(void *dst, const void *src, size_t len);

#else
#warning "AVX2 instructions not available, non-temporal optimzations disabled"
#define bzero_nt(d,l)    memset(d,0,l)
#define memcpy_nt(d,s,l) memcpy(d,s,l)
#endif // HAVE_AVX512F || HAVE_AVX2
#endif // _HPGUPPI_UTIL_H_
