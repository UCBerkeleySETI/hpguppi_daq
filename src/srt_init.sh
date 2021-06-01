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

SCHAN=64
case `hostname` in
  blc00)
    SCHAN=64
    ;;
  blc01)
    SCHAN=128
    ;;
esac

$(dirname $0)/hpguppi_init.sh $runner srt 0 \
  -o TELESCOP=SRT    \
  -o DESTIP=$destip  \
  -o OBSBW=187.5     \
  -o OBSNCHAN=64     \
  -o OBSSCHAN=$SCHAN \
  -o PKTSTART=0      \
  -o DWELL=0         \
  "${@}"

sleep 1

#hashpipe_check_status -k NDROP -i 0

hashpipe_redis_gateway.rb -D srt -g `hostname -s` -i 0
