#include <stdio.h>

#include "hpguppi_udp.h"
#include "transpose.h"

int main(int argc, char *argv[])
{
  int i;
  int errs = 0;
  int c, t;
  const int chan_per_packet = 64;
  const int samp_per_packet = 32;
  const int samp_per_block = 64;

  int __attribute__ ((aligned (64))) packet[2048];
  int __attribute__ ((aligned (64))) dest[4096];

  for(i=0; i<2048; i++) {
    packet[i] = i;
  }

#if 0
  for(i=0; i<8; i++) {
    printf("packet[%d] = %d\n", i, packet[i]);
  }
  printf("\n");
#endif

#if HAVE_AVX512F_INSTRUCTIONS
  hpguppi_data_copy_transpose_avx512
#else
  hpguppi_data_copy_transpose_avx
#endif
  (
    (char *)dest, (char *)packet,
    chan_per_packet,
    samp_per_packet,
    samp_per_block
  );

#if 0
  for(i=0; i<8; i++) {
    printf("dest[%d] = %d\n", i, dest[i]);
  }
  printf("\n");
#endif

  for(t=0; t<samp_per_packet; t++) {
    for(c=0; c<chan_per_packet; c++) {
      if(packet[t*chan_per_packet + c] != dest[c*samp_per_block + t]) {
        printf("mismatch for time=%d chan=%d expected %d got %d\n",
            t, c,
            packet[t*chan_per_packet + c],
            dest[c*samp_per_block + t]
        );
        errs++;
      }
    }
  }
  printf("%d errors detected\n", errs);

  return errs ? 1 : 0;
}
