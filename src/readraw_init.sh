#!/bin/bash
#
# readraw_init.sh - Read GUPPI RAW files
# Usage: readraw_init.sh [mode] [input] [output]
#
#     mode   - readonly|cp
#     input  - input dir
#     output - (optional)
#

#basefile=""         #Input dir
#outdir=${basefile}  #Output dir, replace if wants to write files to a different location
hpguppi_plugin=/home/cherryng/softwares/hpguppi_daq/Dev-branch/hpguppi_daq/src/.libs/hpguppi_daq.so

#Supported modes
if [ "$1" = 'readonly' ]
then
    net_thread=hpguppi_read_raw_files
    out_thread=null_output_thread
    basefile=$2
    outdir=${basefile}
    shift
elif [ "$1" = 'cp' ]
then
    net_thread=hpguppi_read_raw_files
    out_thread=hpguppi_rawdisk_only_thread
    basefile=$2
    outdir=$3
    shift
fi

echo "Run Command:" hashpipe -p ${hpguppi_plugin:-hpguppi_daq} $net_thread $out_thread \
 -o BASEFILE=${basefile} \
 -o OUTDIR=${outdir}
	 

hashpipe -p ${hpguppi_plugin:-hpguppi_daq} $net_thread $out_thread \
 -o BASEFILE=${basefile} \
 -o OUTDIR=${outdir}
	 
