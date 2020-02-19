#!/bin/bash
#
# hpguppi_meerkat.sh - A wrapper around hpguppi_init.sh for running
# hpguppi_daq on MeerKAT.

# These are based on test pcap files from a 4K mode single stream pcap file (OBSFREQ is faked)
fenchan=4096
nants=64
nstrm=1
hntime=256
hnchan=16
schan=2688
chan_bw="0.208984375"
obsbw="3.34375"
obsfreq="1420"
obsnchan=$((hnchan * nstrm * nants))
hclocks=2097152

perf=
if [ "$1" = 'perf' ]
then
  perf=perf
  shift
fi

$(dirname $0)/hpguppi_init.sh $perf meerkat 0 \
  -o DESTIP="0.0.0.0" \
  -o FENCHAN=${fenchan} \
  -o NANTS=${nants} \
  -o NSTRM=${nstrm} \
  -o HNTIME=${hntime} \
  -o HNCHAN=${hnchan} \
  -o SCHAN=${schan} \
  -o CHAN_BW=${chan_bw} \
  -o OBSBW=${obsbw} \
  -o OBSFREQ=${obsfreq} \
  -o OBSNCHAN=${obsnchan} \
  -o HCLOCKS=${hclocks} \
  "${@}"

sleep 1

hashpipe_check_status -k NDROP -i 0

hashpipe_redis_gateway.rb -D bluse -g `hostname -s` -i 0
