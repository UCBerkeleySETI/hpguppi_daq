#!/bin/bash

# Must run as root
if [ $UID -ne 0 ]
then
  exec sudo $0 "${@}"
fi

hosts="${@:-$(echo blc{0..0}{0..7})}"

for host in $hosts
do
  case $host in
    blc0[0-7])
      ;;
    *) echo "skipping unknown host $host"
      continue
      ;;
  esac

  echo "killing hpguppi_daq instances on $host"
  ssh -o BatchMode=yes -o ConnectTimeout=1 $host 'pkill -INT  -f "hashpipe -p hpguppi_daq"'
  ssh -o BatchMode=yes -o ConnectTimeout=1 $host 'pkill -TERM -f "hashpipe -p hpguppi_daq"'
  ssh -o BatchMode=yes -o ConnectTimeout=1 $host 'pkill -KILL -f "hashpipe -p hpguppi_daq"'

  echo "killing hashpipe redis gateway on $host"
  ssh -o BatchMode=yes -o ConnectTimeout=1 $host 'pkill -INT  -f "ruby /usr/local/bin/hashpipe_redis_gateway.rb"'
  ssh -o BatchMode=yes -o ConnectTimeout=1 $host 'pkill -TERM -f "ruby /usr/local/bin/hashpipe_redis_gateway.rb"'
  ssh -o BatchMode=yes -o ConnectTimeout=1 $host 'pkill -KILL -f "ruby /usr/local/bin/hashpipe_redis_gateway.rb"'

  echo "killing hpguppi gbt status loop on $host"
  ssh -o BatchMode=yes -o ConnectTimeout=1 $host 'pkill -INT  -f "ruby /usr/local/bin/hpguppi_gbtstatus_loop.rb"'
  ssh -o BatchMode=yes -o ConnectTimeout=1 $host 'pkill -TERM -f "ruby /usr/local/bin/hpguppi_gbtstatus_loop.rb"'
  ssh -o BatchMode=yes -o ConnectTimeout=1 $host 'pkill -KILL -f "ruby /usr/local/bin/hpguppi_gbtstatus_loop.rb"'

  echo "killing players on $host"
  ssh -o BatchMode=yes -o ConnectTimeout=1 $host 'pkill -INT  -f "python /opt/dibas/lib/python/player.py"'
  ssh -o BatchMode=yes -o ConnectTimeout=1 $host 'pkill -TERM -f "python /opt/dibas/lib/python/player.py"'
  ssh -o BatchMode=yes -o ConnectTimeout=1 $host 'pkill -KILL -f "python /opt/dibas/lib/python/player.py"'
done
