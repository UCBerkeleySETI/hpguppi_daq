# serial 1 coherent_beamformer.m4
AC_DEFUN([AX_CHECK_CBF],
[AC_PREREQ([2.65])dnl
AC_ARG_WITH([coherent_beamformer],
            AC_HELP_STRING([--with-coherent_beamformer=DIR],
                           [Location of CBF files (/home/mruzinda/beamformer_workspace)]),
            [CBFDIR="$withval"],
            [CBFDIR=/home/mruzinda/beamformer_workspace])

orig_LDFLAGS="${LDFLAGS}"
LDFLAGS="${orig_LDFLAGS} -L${CBFDIR}/lib"
AC_CHECK_LIB([coherent_beamformer], [init_beamformer],
             # Found
             AC_SUBST(CBF_LIBDIR,${CBFDIR}/lib),
             # Not found there, check CBFDIR
             AS_UNSET(ac_cv_lib_coherent_beamformer_run_beamformer)
             LDFLAGS="${orig_LDFLAGS} -L${CBFDIR}"
             AC_CHECK_LIB([coherent_beamformer], [init_beamformer],
                          # Found
                          AC_SUBST(CBF_LIBDIR,${CBFDIR}),
                          # Not found there, error
                          AC_MSG_ERROR([CBF library not found])))
LDFLAGS="${orig_LDFLAGS}"

AC_CHECK_FILE([${CBFDIR}/include/coherent_beamformer_char_in.h],
              # Found
              AC_SUBST(CBF_INCDIR,${CBFDIR}/include),
              # Not found there, check CBFDIR
              AC_CHECK_FILE([${CBFDIR}/coherent_beamformer_char_in.h],
                            # Found
                            AC_SUBST(CBF_INCDIR,${CBFDIR}),
                            # Not found there, error
                            AC_MSG_ERROR([coherent_beamformer_char_in.h header file not found])))

])
