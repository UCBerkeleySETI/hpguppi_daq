#!/bin/bash

# Must run as root
if [ $UID -ne 0 ]
then
  exec sudo $0 "${@}"
fi

hosts="${@:-$(echo blc{0..0}{0..7})}"

bank0_names=(A C E G I K M O)
bank1_names=(B D F H J L N P)

datadir=(/datax/dibas /datax2/dibas)

for host in $hosts
do
  case $host in
    blc0[0-7])
      ;;
    *) echo "skipping unknown host $host"
      continue
      ;;
  esac

  for i in 0 1
  do
    echo "starting hashpipe instance $i on $host"
    # Invoke hpguppi_init.sh separately for each instance to facilitate
    # instance specific options later on.
    ssh -o BatchMode=yes -o ConnectTimeout=1 $host hpguppi_init.sh $i -o DATADIR=${datadir[i]}
  done

  echo "starting hashpipe redis gateway on $host"
  ssh -o BatchMode=yes -o ConnectTimeout=1 $host hashpipe_redis_gateway.rb --instances=0,1

  echo "starting hpguppi gbt status loop on $host"
  ssh -o BatchMode=yes -o ConnectTimeout=1 $host hpguppi_gbtstatus_loop.rb --instances=0,1

  # Get compute node number from hostname
  blc=${host#blc}
  # Remove any leading zero
  blc=${blc##0}

  # Get bank names using compute node number
  bank0=${bank0_names[$blc]}
  bank1=${bank1_names[$blc]}

  echo "starting player for BANK${bank0} on $host"
  ssh  -o BatchMode=yes -o ConnectTimeout=1 -n -f $host "sh -c 'cd /datax;
    taskset -c 7-11 nohup /opt/dibas/bin/player BANK${bank0} </dev/null >${host/blc/blp}.0.out 2>${host/blc/blp}.0.err &'"

  echo "starting player for BANK${bank1} on $host"
  ssh  -o BatchMode=yes -o ConnectTimeout=1 -n -f $host "sh -c 'cd /datax2;
    taskset -c 7-11 nohup /opt/dibas/bin/player BANK${bank1} </dev/null >${host/blc/blp}.1.out 2>${host/blc/blp}.1.err &'"
done
