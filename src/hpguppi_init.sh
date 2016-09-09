#!/bin/bash

# Add directory containing this script to PATH
PATH="$(dirname $0):${PATH}"

hostname=`hostname -s`

instances=(
  # Setup parameters for two instances.
  #
  # We use a dual hexa-core CPU system with everything on the second socket (CPUs 6-11),
  # with CPU 6 being reserved for NIC IRQs.  This leads to the following CPU mask:
  #
  # 1100 0000 0000
  # 1098 7654 3210
  # ---- ---- ----
  # 1111 1000 0000 = 0xf80
  #
  #       bind  NET  OUT
  # mask  host  CPU  CPU
  "0xf80  eth4   7    8"  # Instance 0, eth4
  "0xf80  eth5   9   10"  # Instance 1, eth5
)

function init() {
  instance=$1
  mask=$2
  bindhost=$3
  netcpu=$4
  outcpu=$5

  if [ -z "${mask}" ]
  then
    echo "Invalid instance number '${instance}' (ignored)"
    return 1
  fi

  if [ -z "$outcpu" ]
  then
    echo "Invalid configuration for host ${hostname} instance ${instance} (ignored)"
    return 1
  fi

  echo taskset $mask \
  hashpipe -p hpguppi_daq -I $instance \
    -o BINDHOST=$bindhost \
    -o BINDPORT=60000 \
    -c $netcpu hpguppi_net_thread \
    -c $outcpu $out_thread

  taskset $mask \
  hashpipe -p hpguppi_daq -I $instance \
    -o BINDHOST=$bindhost \
    -o BINDPORT=60000 \
    -c $netcpu $net_thread \
    -c $outcpu $out_thread \
     < /dev/null \
    1> ${hostname}.$instance.out \
    2> ${hostname}.$instance.err &
}

if [ -z "$1" ]
then
  echo "Usage: $(basename $0) INSTANCE_ID [...]"
  exit 1
fi

net_thread=hpguppi_net_thread
out_thread=hpguppi_rawdisk_thread

if [ "$1" = 'fakefake' ]
then
  net_thread=hpguppi_fake_net_thread
  out_thread=null_output_thread
  shift
elif [ "$1" = 'fake' ]
then
  net_thread=hpguppi_fake_net_thread
  shift
elif echo "$1" | grep -q 'thread'
then
  out_thread="$1"
  shift
fi

echo using net_thread $net_thread
echo using out_thread $out_thread

for instidx in "$@"
do
  args="${instances[$instidx]}"
  if [ -n "${args}" ]
  then
    echo
    echo Starting instance $hostname/$instidx
    init $instidx $args
    echo Instance $hostname/$instidx pid $!
    # Sleep to let instance come up
    sleep 5
  else
    echo Instance $instidx not defined for host $hostname
  fi
done
