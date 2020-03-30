ACLOCAL_AMFLAGS = -I m4
AM_CPPFLAGS     =

AM_CPPFLAGS += -I"@HASHPIPE_INCDIR@"

# AM_CFLAGS is used for all C compiles
AM_CFLAGS = -ggdb -fPIC -O3 -Wall -Werror -fno-strict-aliasing -mavx512f
#AM_CFLAGS = -ggdb -fPIC -O0 -Wall -Werror -fno-strict-aliasing -mavx512f

# Convenience variables to group source files
hpguppi_databuf = hpguppi_databuf.h \
		  hpguppi_databuf.c

hpguppi_support = hpguppi_params.h \
		  hpguppi_params.c \
		  hpguppi_mkfeng.h \
		  hpguppi_pksuwl.h \
		  hpguppi_rawspec.h \
		  hpguppi_rawspec.c \
		  hpguppi_time.h   \
		  hpguppi_time.c   \
		  hpguppi_udp.h    \
		  hpguppi_udp.c    \
		  hpguppi_vdif.h   \
		  polyco_struct.h  \
		  psrfits.h

libsla_support = slalib.h sla.c f77.h

hpguppi_threads = hpguppi_ibverbs_pkt_thread.c \
		  hpguppi_net_thread.c \
		  hpguppi_mb1_net_thread.c \
		  hpguppi_mb128ch_net_thread.c \
		  hpguppi_meerkat_net_thread.c \
		  hpguppi_meerkat_pkt_thread.c \
		  hpguppi_meerkat_spead_thread.c \
		  hpguppi_pksuwl_net_thread.c \
		  hpguppi_rawdisk_thread.c \
		  hpguppi_rawdisk_only_thread.c \
		  hpguppi_fildisk_only_thread.c

# This is the paper_gpu plugin itself
lib_LTLIBRARIES = hpguppi_daq.la
hpguppi_daq_la_SOURCES  = $(hpguppi_databuf) $(hpguppi_support) $(libsla_support) $(hpguppi_threads)
hpguppi_daq_la_LIBADD   = -lsla -lrt -lrawspec -lhashpipe_ibverbs
hpguppi_daq_la_LDFLAGS  = -avoid-version -module -shared -export-dynamic
hpguppi_daq_la_LDFLAGS += -L"@LIBSLADIR@" -Wl,-rpath,"@LIBSLADIR@"
hpguppi_daq_la_LDFLAGS += -L"@HASHPIPE_LIBDIR@" -Wl,-rpath,"@HASHPIPE_LIBDIR@"

# Installed scripts
dist_bin_SCRIPTS = hpguppi_gbtstatus_loop.rb \
		   hpguppi_init.sh           \
		   kill_hpguppi.sh           \
		   start_hpguppi.sh

check_PROGRAMS = test_vdifhdr

test_vdifhdr_SOURCES = test_vdifhdr.c hpguppi_pksuwl.h hpguppi_vdif.h

# vi: set ts=8 noet :
