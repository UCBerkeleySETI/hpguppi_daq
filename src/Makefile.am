ACLOCAL_AMFLAGS = -I m4
AM_CPPFLAGS     =

AM_CPPFLAGS += -I"@HASHPIPE_INCDIR@"

# AM_CFLAGS is used for all C compiles
AM_CFLAGS = -ggdb -fPIC -O3 -Wall -Werror -fno-strict-aliasing -mavx

# Convenience variables to group source files
hpguppi_databuf = hpguppi_databuf.h \
		  hpguppi_databuf.c

hpguppi_support = hpguppi_params.h \
		  hpguppi_params.c \
		  hpguppi_time.h   \
		  hpguppi_time.c   \
		  hpguppi_udp.h    \
		  hpguppi_udp.c    \
		  polyco_struct.h  \
		  psrfits.h

libsla_support = slalib.h sla.c f77.h

hpguppi_threads = hpguppi_net_thread.c \
		  hpguppi_rawdisk_thread.c

# This is the paper_gpu plugin itself
lib_LTLIBRARIES = hpguppi_daq.la
hpguppi_daq_la_SOURCES  = $(hpguppi_databuf) $(hpguppi_support) $(libsla_support) $(hpguppi_threads)
hpguppi_daq_la_LIBADD   = -lsla -lrt
hpguppi_daq_la_LDFLAGS  = -avoid-version -module -shared -export-dynamic
hpguppi_daq_la_LDFLAGS += -L"@LIBSLADIR@" -Wl,-rpath,"@LIBSLADIR@"
hpguppi_daq_la_LDFLAGS += -L"@HASHPIPE_LIBDIR@" -Wl,-rpath,"@HASHPIPE_LIBDIR@"

# Installed scripts
dist_bin_SCRIPTS = hpguppi_init.sh

# vi: set ts=8 noet :
