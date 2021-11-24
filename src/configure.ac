#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.65])
AC_INIT([FULL-PACKAGE-NAME], [VERSION], [BUG-REPORT-ADDRESS])
AM_INIT_AUTOMAKE([foreign])
LT_INIT
AM_SILENT_RULES([yes])
AC_CONFIG_SRCDIR([hpguppi_databuf.h])
AC_CONFIG_HEADERS([config.h])
AC_CONFIG_MACRO_DIR([m4])

# Set CFLAGS to nothing if it is not set by the user.  This prevents AC_PROG_CC
# from setting the (supposedly reserved-for-the-user!) variable CFLAGS in
# Makefile, which prevents AM_CFLAGS in Makefile.am from setting an
# optimization level.  For more details, see
# http://lists.gnu.org/archive/html/autoconf/2006-04/msg00007.html
AS_VAR_SET_IF(CFLAGS,[],[CFLAGS=])

# Checks for programs.
AC_PROG_CC

# Check processor flags
AX_CHECK_X86_FEATURES
AM_CONDITIONAL([HAVE_AVX512F],
               [test "x$ax_cv_gcc_x86_cpu_supports_avx512f" = xyes])

# Check for HASHPIPE
AX_CHECK_HASHPIPE

# Check for rawspec
AX_CHECK_RAWSPEC

# Check for coherent beamformer
AX_CHECK_CBF

# Check for python3.7m
AX_CHECK_PY

# Checks for libraries.
#AC_CHECK_LIB([pthread], [pthread_create])
AC_CHECK_LIB([m], [cos])
#AC_CHECK_LIB([rt], [clock_gettime])
#AC_CHECK_LIB([z], [crc32])

AC_ARG_WITH([libcoherent_beamformer],
            AC_HELP_STRING([--with-libcoherent_beamformer=DIR],
                           [Location of CBF library]),
            [LIBCBFDIR="$withval"],
            [LIBCBFDIR="."])

AC_ARG_WITH([libpython3.7m],
            AC_HELP_STRING([--with-libpython3.7m=DIR],
                           [Location of python3.7 library]),
            [LIBPYDIR="$withval"],
            [LIBPYDIR="."])

AC_ARG_WITH([libsla],
            AC_HELP_STRING([--with-libsla=DIR],
                           [Location of SLA library]),
            [LIBSLADIR="$withval"],
            [LIBSLADIR="."])

orig_LDFLAGS="${LDFLAGS}"
LDFLAGS="${orig_LDFLAGS} -L${LIBSLADIR}"
AC_CHECK_LIB([sla], [sla_obs_],
             # Found
             AC_SUBST(LIBSLADIR,${LIBSLADIR}),
             # Not found there, error
             AC_MSG_ERROR([SLA library not found]))
LDFLAGS="${orig_LDFLAGS}"

## Checks for header files.
#AC_CHECK_HEADERS([netdb.h stdint.h stdlib.h string.h sys/socket.h sys/time.h unistd.h zlib.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_C_INLINE
AC_TYPE_INT32_T
AC_TYPE_INT64_T
AC_TYPE_OFF_T
AC_TYPE_SIZE_T
AC_TYPE_UINT32_T
AC_TYPE_UINT64_T
AC_TYPE_UINT8_T

## Checks for library functions.
#AC_FUNC_MALLOC
#AC_CHECK_FUNCS([clock_gettime memset socket crc32])

AC_CONFIG_FILES([Makefile])
AC_OUTPUT
