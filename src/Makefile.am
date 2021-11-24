ACLOCAL_AMFLAGS = -I m4
AM_CPPFLAGS     =

AM_CPPFLAGS += -I"@HASHPIPE_INCDIR@"
AM_CPPFLAGS += -I"@CBF_INCDIR@"
AM_CPPFLAGS += -I"@PY_INCDIR@" # For embedding katpoint and mosiac modules

# AM_CFLAGS is used for all C compiles
AM_CFLAGS = -ggdb -fPIC -O3 -Wall -Werror -Wno-error=cpp -fno-strict-aliasing @X86_FEATURE_CFLAGS@

# Convenience variables to group source files
hpguppi_databuf = hpguppi_databuf.h \
		  hpguppi_databuf.c

hpguppi_support = hpguppi_ibverbs_pkt_thread.h \
		  hpguppi_params.c \
		  hpguppi_mkfeng.h \
		  hpguppi_pksuwl.h \
		  hpguppi_rawspec.h \
		  hpguppi_rawspec.c \
		  hpguppi_time.h   \
		  hpguppi_time.c   \
		  hpguppi_util.h   \
		  hpguppi_util.c   \
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
		  hpguppi_pksuwl_vdif_thread.c \
		  hpguppi_rawdisk_thread.c \
		  hpguppi_rawdisk_only_thread.c \
		  hpguppi_fildisk_meerkat_thread.c \
		  hpguppi_fildisk_only_thread.c \
		  hpguppi_rawfile_input_thread.c \
		  hpguppi_coherent_bf_thread.c

# This is the hpguppi_daq plugin
lib_LTLIBRARIES = hpguppi_daq.la
hpguppi_daq_la_SOURCES  = $(hpguppi_databuf) $(hpguppi_support) $(libsla_support) $(hpguppi_threads)
#hpguppi_daq_la_LIBADD   = -lsla -lrt -lrawspec -lhashpipe_ibverbs -lcoherent_beamformer -lm -L/opt/conda/lib -lpython3.7m
hpguppi_daq_la_LIBADD   = -lsla -lrt -lrawspec -lhashpipe_ibverbs -lcoherent_beamformer -lm -lpython3.7m
hpguppi_daq_la_LDFLAGS  = -avoid-version -module -shared -export-dynamic
hpguppi_daq_la_LDFLAGS += -L"@LIBSLADIR@" -Wl,-rpath,"@LIBSLADIR@"
hpguppi_daq_la_LDFLAGS += -L"@HASHPIPE_LIBDIR@" -Wl,-rpath,"@HASHPIPE_LIBDIR@"
hpguppi_daq_la_LDFLAGS += -L"@CBF_LIBDIR@" -Wl,-rpath,"@CBF_LIBDIR@"
hpguppi_daq_la_LDFLAGS += -L"@PY_LIBDIR@" -Wl,-rpath,"@PY_LIBDIR@"

# Installed scripts
dist_bin_SCRIPTS = hpguppi_gbtstatus_loop.rb \
		   hpguppi_init.sh           \
		   kill_hpguppi.sh           \
		   start_hpguppi.sh

check_PROGRAMS = test_vdifhdr

test_vdifhdr_SOURCES = test_vdifhdr.c hpguppi_pksuwl.h hpguppi_vdif.h

# vi: set ts=8 noet :
