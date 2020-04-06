// hpguppi_util.c
//
// Utility functions for hpguppi_daq

#include <errno.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <immintrin.h>

#include "hpguppi_util.h"

int
mkdir_p(char *pathname, mode_t mode)
{
  char *p = pathname;

  if(!pathname) {
    errno = EINVAL;
    return -1;
  }

  // If absolute path is given, move past leading '/'
  if(*p == '/') {
    p++;
  }

  // Find first (non-root) slash, if any
  p = strchr(p, '/');

  // Make sure all parent diretories exist
  while(p && *p) {
    // NUL terminate at '/'
    *p = '\0';

    // Make directory, ignore EEXIST errors
    if(mkdir(pathname, mode) && errno != EEXIST) {
      return -1;
    }

    // Restore '/' and advance p to next character
    *p++ = '/';

    // Find next slash
    p = strchr(p, '/');
  }

  // Make directory, ignore EEXIST errors
  if(mkdir(pathname, mode) && errno != EEXIST) {
    return -1;
  }

  return 0;
}

// Implementations of functions requiring AVX512F or AVX2

#if HAVE_AVX512F_INSTRUCTIONS || HAVE_AVX2_INSTRUCTIONS

#if HAVE_AVX512F_INSTRUCTIONS
#define TYPE_NT  __m512i
#define LOAD_NT  _mm512_stream_load_si512
#define STORE_NT _mm512_stream_si512
#define ZERO_NT  _mm512_setzero_si512
#define SIZE_NT  6
#elif HAVE_AVX2_INSTRUCTIONS
#define TYPE_NT  __m256i
#define LOAD_NT  _mm256_stream_load_si256
#define STORE_NT _mm256_stream_si256
#define ZERO_NT  _mm256_setzero_si256
#define SIZE_NT  5
#endif

void
bzero_nt(void * dst, size_t len)
{
  // Create wide zero value
  const TYPE_NT zero = ZERO_NT();

  // Cast dst to TYPE_NT pointer
  TYPE_NT * pwide = (TYPE_NT *)dst;

  // Convert len from 1 byte units to wide units
  len >>= SIZE_NT;

  // While len > 0
  while(len) {
    *pwide++ = zero;
    len--;
  }
}

void
memcpy_nt(void *dst, const void *src, size_t len)
{
  // Cast dst and src to pointer to wide type
  TYPE_NT *dstwide = (TYPE_NT *)dst;
  TYPE_NT *srcwide = (TYPE_NT *)src;

  // Convert len from 1 byte units to wide units
  len >>= SIZE_NT;

  while(len--) {
    STORE_NT(dstwide++, LOAD_NT(srcwide++));
  }
}

#endif // HAVE_AVX512F || HAVE_AVX2
