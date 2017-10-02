#!/bin/bash

# Add directory containing this script to PATH
PATH="$(dirname $0):${PATH}"

hostname=`hostname -s`

cores_per_cpu=`grep cpu.cores /proc/cpuinfo | awk 'NR==1{print $NF}'`

case $cores_per_cpu in
  6)
    # We use a dual hexa-core CPU system with everything on the second socket (CPUs 6-11),
    # with CPU 6 being reserved for NIC IRQs.  This leads to the following CPU mask:
    #
    # 1111 1100 0000 0000
    # 5432 1098 7654 3210
    # ---- ---- ---- ----
    # 0000 1111 1000 0000 = 0x0f80
    NET0CPU=7
    OUT0CPU=8
    NET1CPU=9
    OUT1CPU=10
    MASK=0x0f80
    ;;
  8)
    # We use a dual octa-core CPU system with everything on the second socket (CPUs 8-15),
    # with CPU 8 being reserved for NIC IRQs.  This leads to the following CPU mask:
    #
    # 1111 1100 0000 0000
    # 5432 1098 7654 3210
    # ---- ---- ---- ----
    # 1111 1110 0000 0000 = 0xfe00
    NET0CPU=9
    OUT0CPU=10
    NET1CPU=11
    OUT1CPU=12
    MASK=0xfe00
    ;;
  *)
    echo "$cores_per_cpu cores per cpu is not yet supported by $0"
    exit 1
    ;;
esac

instances=(
  # Setup parameters for two instances.
  #
  #
  #                bind     NET       OUT
  #  dir     mask  host     CPU       CPU
  "/datax   $MASK  eth4  $NET0CPU  $OUT0CPU"  # Instance 0, eth4
  "/datax2  $MASK  eth5  $NET1CPU  $OUT1CPU"  # Instance 1, eth5
)

function init() {
  instance=$1
  dir=$2
  mask=$3
  bindhost=$4
  netcpu=$5
  outcpu=$6
  shift 6

  if [ -z "${dir}" ]
  then
    echo "Invalid instance number '${instance:-[unspecified]}' (ignored)"
    return 1
  fi

  if [ -z "$outcpu" ]
  then
    echo "Invalid configuration for host ${hostname} instance ${instance} (ignored)"
    return 1
  fi

  if ! cd "${dir}"
  then
    echo "Invalid working directory ${dir} (ignored)"
    return 1
  fi

  echo taskset $mask \
  hashpipe -p hpguppi_daq -I $instance \
    -o BINDHOST=$bindhost \
    -o BINDPORT=60000 \
    -o DATADIR=$dir \
    ${@} \
    -c $netcpu hpguppi_net_thread \
    -c $outcpu $out_thread

  taskset $mask \
  hashpipe -p hpguppi_daq -I $instance \
    -o BINDHOST=$bindhost \
    -o BINDPORT=60000 \
    -o DATADIR=$dir \
    ${@} \
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

instance_ids=''

while [ -n "$1" ]
do
  # Break on first option character
  case $1 in
    -*) break;;
  esac

  instance_ids="${instance_ids} ${1}"
  shift
done

# Get sync time from redis
sync_time=`redis-cli -h redishost get sync_time | tr -d '"'`

for instidx in $instance_ids
do
  fifo="/tmp/hpguppi_daq_control/$instidx"
  args="${instances[$instidx]}"
  if [ -n "${args}" ]
  then
    echo Starting instance $hostname/$instidx
    if init $instidx $args "${@}"
    then
      echo Instance $hostname/$instidx pid $!
      hashpipe_check_status -I $instidx -k SYNCTIME -i ${sync_time:-0}

      # Loop until control fifo exists, time out after 3 seconds
      timeout=3
      while [ $timeout -gt 0 -a ! -e $fifo ]
      do
        timeout=$((timeout-1))
        sleep 1
      done

      # If FIFO exists, use it to "start" the instance
      # (i.e. put it in the armed state)
      if [ -e $fifo ]
      then
        echo start > $fifo
      else
        echo "Error: control FIFO not created within 3 seconds"
      fi
    fi
  else
    echo Instance $instidx not defined for host $hostname
  fi
done
