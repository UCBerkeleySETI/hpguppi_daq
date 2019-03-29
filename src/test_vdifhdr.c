#include <stdio.h>

#include "hpguppi_vdif.h"
#include "hpguppi_pksuwl.h"

int main(int argc, char *argv[])
{
  int i;

  struct tm tm;
  struct timespec ts;

  uint8_t vdifhdr_bytes[] = {
    0x84, 0xdf, 0xb0, 0x00,
    0xd3, 0xb9, 0x00, 0x25,
    0x04, 0x04, 0x00, 0x20,
    0x4b, 0x50, 0x01, 0xbc,
    0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00,
    0xcd, 0xd7, 0x49, 0x00
  };

  // The VDIF header above will produce the following output:
  //
  // - invalid: 0
  //   legacy: 0
  //   ref_epoch_secs: 11591556
  //   ref_epoch: 37
  //   data_frame_seq: 47571
  //   vdif_version: 1
  //   log2_nchan: 0
  //   data_frame_length: 8224
  //   data_array_length: 8192
  //   complex: 1
  //   bits_per_sample: 16
  //   thread_id: 1
  //   station_id: 20555
  //   extended_data_version: 0
  //   extended_data: [0, 0, 0, 4839373]
  //   ref_epoch_tm:
  //     year: 2018
  //     month: 7
  //     day: 1
  //   time: 1541994756
  //   timespec:
  //     sec: 1541994756
  //     nsec: 761136000

  struct vdifhdr * pvdifhdr = (struct vdifhdr *)vdifhdr_bytes;

  printf("- invalid: %u\n", vdif_get_invalid(pvdifhdr));
  printf("  legacy: %u\n", vdif_get_legacy(pvdifhdr));
  printf("  ref_epoch_secs: %u\n", vdif_get_ref_epoch_secs(pvdifhdr));

  printf("  ref_epoch: %u\n", vdif_get_ref_epoch(pvdifhdr));
  printf("  data_frame_seq: %u\n", vdif_get_data_frame_seq(pvdifhdr));

  printf("  vdif_version: %u\n", vdif_get_vdif_version(pvdifhdr));
  printf("  log2_nchan: %u\n", vdif_get_log2_nchan(pvdifhdr));
  printf("  data_frame_length: %u\n", vdif_get_data_frame_length(pvdifhdr));
  printf("  data_array_length: %u\n", vdif_get_data_array_length(pvdifhdr));

  printf("  complex: %u\n", vdif_get_complex(pvdifhdr));
  printf("  bits_per_sample: %u\n", vdif_get_bits_per_sample(pvdifhdr));
  printf("  thread_id: %u\n", vdif_get_thread_id(pvdifhdr));
  printf("  station_id: %u\n", vdif_get_station_id(pvdifhdr));

  printf("  extended_data_version: %u\n", vdif_get_extended_data_version(pvdifhdr));
  printf("  extended_data: [%u", vdif_get_extended_data(pvdifhdr, 0));
  for(i=1; i<4; i++) {
    printf(", %u", vdif_get_extended_data(pvdifhdr, i));
  }
  printf("]\n");

  vdif_get_ref_epoch_tm(pvdifhdr, &tm);
  printf("  ref_epoch_tm:\n");
  // Add 1900 to get to CE years
  printf("    year: %u\n", tm.tm_year + 1900);
  // Add 1 to get to 1 for Jan, 7 for July
  printf("    month: %u\n", tm.tm_mon + 1);
  printf("    day: %u\n", tm.tm_mday);

  printf("  time: %ld\n", vdif_get_time(pvdifhdr));

  vdif_get_timespec(pvdifhdr, &ts, 1000UL*2048/128);
  printf("  timespec:\n");
  printf("    sec: %ld\n", ts.tv_sec);
  printf("    nsec: %ld\n", ts.tv_nsec);

  printf("  pksuwl_pktidx: %lu\n", pksuwl_get_pktidx(pvdifhdr));

  return 0;
}
