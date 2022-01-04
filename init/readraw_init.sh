#!/bin/bash
#
# readraw_init.sh - Read GUPPI RAW files
# Usage: readraw_init.sh [mode] [input] [output]
#
#     mode   - readonly|cp|fil|cbf
#     input  - input dir
#     output - (optional)
#

hpguppi_plugin=../src/.libs/hpguppi_daq.so

#Supported modes
if [ "$1" = 'readonly' ]
then
    net_thread=hpguppi_rawfile_input_thread
    out_thread=null_output_thread
    if [ -z "$2" ]
    then
        echo Input dir not provided.
        echo Usage: readraw_init.sh readonly [input]
        echo Exiting...
        exit
    else
        # Path to file in first argument of execution
        path_to_raw_files=$2

        # Finds the oldest file in the directory
        oldest_file=$(ls $path_to_raw_files -Art | head -n 1)

        # Removes everything after the first period in the RAW file name
        basefile=$path_to_raw_files"/"${oldest_file%%.*}
        #basefile=$2
        outdir=${basefile}
        shift
    fi
elif [ "$1" = 'cp' ] || [ "$1" = 'fil' ] || [ "$1" = 'cbf' ]
then
    net_thread=hpguppi_rawfile_input_thread
    if [ "$1" = 'cp' ]
    then
        out_thread=hpguppi_rawdisk_only_thread
    elif [ "$1" = 'fil' ]
    then
        out_thread=hpguppi_fildisk_meerkat_thread
    else
	out_thread=hpguppi_coherent_bf_thread
    fi

    if [ -z "$2" ] || [ -z "$3" ]
    then
        echo Input/Output dir not provided.
        echo Usage: readraw_init.sh [mode] [input] [output]
        echo Exiting...
        exit
    else
        # Path to file in first argument of execution
        path_to_raw_files=$2

        # Finds the oldest file in the directory
        oldest_file=$(ls $path_to_raw_files -Art | head -n 1)

        # Removes everything after the first period in the RAW file name
        basefile=$path_to_raw_files"/"${oldest_file%%.*}
        #basefile=$2
        outdir=$3
        shift
    fi
else
    echo 'Unsupported mode. Choose between "readonly", "cp", "fil", "cbf".'
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

