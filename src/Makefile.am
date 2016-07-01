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
hpguppi_daq_la_SOURCES  = $(hpguppi_databuf) $(hpguppi_support) $(libsla_support)
#hpguppi_daq_la_SOURCES  = $(hpguppi_databuf) $(hpguppi_threads)
#hpguppi_daq_la_LIBADD   = -lrt
hpguppi_daq_la_LIBADD   = -lsla
hpguppi_daq_la_LDFLAGS  = -avoid-version -module -shared -export-dynamic
hpguppi_daq_la_LDFLAGS += -L"@LIBSLADIR@" -Wl,-rpath,"@LIBSLADIR@"
hpguppi_daq_la_LDFLAGS += -L"@HASHPIPE_LIBDIR@" -Wl,-rpath,"@HASHPIPE_LIBDIR@"

## Installed scripts
#dist_bin_SCRIPTS = paper_init.sh \
#		   paper_crc_init.sh \
#		   xtor_down.sh \
#		   xtor_up.sh

## Non-installed test programs
#noinst_PROGRAMS = paper_fluff_test
#paper_fluff_test_SOURCES = paper_fluff_test.c $(paper_fluff)
## The paper_fluff_test_CFLAGS is an autotools workaround.
## http://www.gnu.org/software/automake/manual/html_node/Objects-created-both-with-libtool-and-without.html
#paper_fluff_test_CFLAGS = $(AM_CFLAGS)
#paper_fluff_test_LDADD = -lrt

# vi: set ts=8 noet :
