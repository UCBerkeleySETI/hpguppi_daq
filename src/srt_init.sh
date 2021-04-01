#!/bin/bash
#
# srt_init.sh - A wrapper around hpguppi_init.sh for running hpguppi_daq on
#               SRT.

runner=
if [ "$1" = 'perf' ]
then
  runner=perf
  shift
fi

destip=`ifconfig eth4 | awk '/inet addr/{print substr($2,6)}'`

$(dirname $0)/hpguppi_init.sh $runner srt 0 \
  -o DESTIP=$destip \
  "${@}"

sleep 1

#hashpipe_check_status -k NDROP -i 0

hashpipe_redis_gateway.rb -D srt -g `hostname -s` -i 0
