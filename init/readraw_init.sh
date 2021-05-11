#!/bin/bash
#
# readraw_init.sh - Read GUPPI RAW files
# Usage: readraw_init.sh [mode] [input] [output]
#
#     mode   - readonly|cp|fil
#     input  - input dir
#     output - (optional)
#

hpguppi_plugin=/home/cherryng/softwares/hpguppi_daq/Dev-branch/hpguppi_daq/src/.libs/hpguppi_daq.so

#Supported modes
if [ "$1" = 'readonly' ]
then
    net_thread=hpguppi_read_raw_files
    out_thread=null_output_thread
    if [ -z "$2" ]
    then
	echo Input dir not provided.
	echo Usage: readraw_init.sh readonly [input]
	echo Exiting...
       exit
    else
	basefile=$2
	outdir=${basefile}
	shift
    fi
elif [ "$1" = 'cp' ]
then
    net_thread=hpguppi_read_raw_files
    out_thread=hpguppi_rawdisk_only_thread
    if [ -z "$2" ] || [ -z "$3" ]
    then
	echo Input/Output dir not provided.
	echo Usage: readraw_init.sh cp [input] [output]
	echo Exiting...
	exit
    else
	basefile=$2
	outdir=$3
	shift
    fi
else
    echo Supported mode. Choose between \"readonly\" and \"cp\".
    echo Exiting...
    exit
fi

#-------------------------------------------
echo "Run Command:" hashpipe -p ${hpguppi_plugin:-hpguppi_daq} $net_thread $out_thread \
 -o BASEFILE=${basefile} \
 -o OUTDIR=${outdir}
	 

hashpipe -p ${hpguppi_plugin:-hpguppi_daq} $net_thread $out_thread \
 -o BASEFILE=${basefile} \
 -o OUTDIR=${outdir}
	 
