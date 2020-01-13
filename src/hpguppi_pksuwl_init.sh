#!/bin/bash
#
# hpguppi_pksuwl_init.sh - A wrapper around hpguppi_init.sh for running
# hpguppi_daq with the Parkes Ultra-Wideband-Low receiver.

# These assaciative arrays are essentially a bashification of the settings
# specified here:
#
# https://github.com/UCBerkeleySETI/pks_seti/blob/master/uwl_control/ip_config.txt

# Compute node specific IP multicast group to join to get data
declare -A pksuwl_destip
pksuwl_destip[blc00]='239.17.12.1'
pksuwl_destip[blc01]='239.17.11.0'
pksuwl_destip[blc02]='239.17.11.1'
pksuwl_destip[blc03]='239.17.11.2'
pksuwl_destip[blc04]='239.17.11.3'
pksuwl_destip[blc05]='239.17.12.0'
pksuwl_destip[blc06]='239.17.12.1'
pksuwl_destip[blc07]='239.17.12.2'
pksuwl_destip[blc10]='239.17.12.3'
pksuwl_destip[blc11]='239.17.11.4'
pksuwl_destip[blc12]='239.17.11.5'
pksuwl_destip[blc13]='239.17.11.6'
pksuwl_destip[blc14]='239.17.11.7'
pksuwl_destip[blc15]='239.17.12.4'
pksuwl_destip[blc16]='239.17.12.5'
pksuwl_destip[blc17]='239.17.12.6'
pksuwl_destip[blc20]='239.17.12.7'
pksuwl_destip[blc21]='239.17.11.8'
pksuwl_destip[blc22]='239.17.11.9'
pksuwl_destip[blc23]='239.17.11.10'
pksuwl_destip[blc24]='239.17.11.11'
pksuwl_destip[blc25]='239.17.12.8'
pksuwl_destip[blc26]='239.17.12.9'
pksuwl_destip[blc27]='239.17.12.10'
pksuwl_destip[blc30]='239.17.12.11'
pksuwl_destip[blc31]='239.17.11.12'
pksuwl_destip[blc32]='239.17.11.13'

# Compute node UDP destination port
declare -A pksuwl_bindport
pksuwl_bindport[blc00]='17205'
pksuwl_bindport[blc01]='17200'
pksuwl_bindport[blc02]='17201'
pksuwl_bindport[blc03]='17202'
pksuwl_bindport[blc04]='17203'
pksuwl_bindport[blc05]='17204'
pksuwl_bindport[blc06]='17205'
pksuwl_bindport[blc07]='17206'
pksuwl_bindport[blc10]='17207'
pksuwl_bindport[blc11]='17208'
pksuwl_bindport[blc12]='17209'
pksuwl_bindport[blc13]='17210'
pksuwl_bindport[blc14]='17211'
pksuwl_bindport[blc15]='17212'
pksuwl_bindport[blc16]='17213'
pksuwl_bindport[blc17]='17214'
pksuwl_bindport[blc20]='17215'
pksuwl_bindport[blc21]='17216'
pksuwl_bindport[blc22]='17217'
pksuwl_bindport[blc23]='17218'
pksuwl_bindport[blc24]='17219'
pksuwl_bindport[blc25]='17220'
pksuwl_bindport[blc26]='17221'
pksuwl_bindport[blc27]='17222'
pksuwl_bindport[blc30]='17223'
pksuwl_bindport[blc31]='17224'
pksuwl_bindport[blc32]='17225'

# Compute node specific OBSBW values
declare -A pksuwl_obsbw
pksuwl_obsbw[blc00]='-128'
pksuwl_obsbw[blc01]='+128'
pksuwl_obsbw[blc02]='+128'
pksuwl_obsbw[blc03]='+128'
pksuwl_obsbw[blc04]='+128'
pksuwl_obsbw[blc05]='+128'
pksuwl_obsbw[blc06]='-128'
pksuwl_obsbw[blc07]='-128'
pksuwl_obsbw[blc10]='-128'
pksuwl_obsbw[blc11]='-128'
pksuwl_obsbw[blc12]='-128'
pksuwl_obsbw[blc13]='-128'
pksuwl_obsbw[blc14]='-128'
pksuwl_obsbw[blc15]='-128'
pksuwl_obsbw[blc16]='-128'
pksuwl_obsbw[blc17]='-128'
pksuwl_obsbw[blc20]='-128'
pksuwl_obsbw[blc21]='-128'
pksuwl_obsbw[blc22]='-128'
pksuwl_obsbw[blc23]='-128'
pksuwl_obsbw[blc24]='-128'
pksuwl_obsbw[blc25]='-128'
pksuwl_obsbw[blc26]='-128'
pksuwl_obsbw[blc27]='-128'
pksuwl_obsbw[blc30]='-128'
pksuwl_obsbw[blc31]='-128'
pksuwl_obsbw[blc32]='-128'

# Compute node specific OBSFREQ values
declare -A pksuwl_obsfreq
pksuwl_obsfreq[blc00]='1408'
pksuwl_obsfreq[blc01]='768'
pksuwl_obsfreq[blc02]='896'
pksuwl_obsfreq[blc03]='1024'
pksuwl_obsfreq[blc04]='1152'
pksuwl_obsfreq[blc05]='1280'
pksuwl_obsfreq[blc06]='1408'
pksuwl_obsfreq[blc07]='1536'
pksuwl_obsfreq[blc10]='1664'
pksuwl_obsfreq[blc11]='1792'
pksuwl_obsfreq[blc12]='1920'
pksuwl_obsfreq[blc13]='2048'
pksuwl_obsfreq[blc14]='2176'
pksuwl_obsfreq[blc15]='2304'
pksuwl_obsfreq[blc16]='2432'
pksuwl_obsfreq[blc17]='2560'
pksuwl_obsfreq[blc20]='2688'
pksuwl_obsfreq[blc21]='2816'
pksuwl_obsfreq[blc22]='2944'
pksuwl_obsfreq[blc23]='3072'
pksuwl_obsfreq[blc24]='3200'
pksuwl_obsfreq[blc25]='3328'
pksuwl_obsfreq[blc26]='3456'
pksuwl_obsfreq[blc27]='3584'
pksuwl_obsfreq[blc30]='3712'
pksuwl_obsfreq[blc31]='3840'
pksuwl_obsfreq[blc32]='3968'

hostname=`hostname`
destip=${pksuwl_destip[$hostname]}
bindport=${pksuwl_bindport[$hostname]}
obsbw=${pksuwl_obsbw[$hostname]}
obsfreq=${pksuwl_obsfreq[$hostname]}

if [ -z "${destip}" ]
then
  echo "unsupported host: ${hostname}"
  exit 1
fi

$(dirname $0)/hpguppi_init.sh pksuwl 0 \
  -o DESTIP=${destip} \
  -o BINDPORT=${bindport} \
  -o OBSBW=${obsbw} \
  -o OBSFREQ=${obsfreq} \
  -o NBITS=16 \
  "${@}"

sleep 1

hashpipe_check_status -k NDROP -i 0
