/* hpguppi_time.h
 *
 * Routines to deal with time convsrsions, etc.
 */
#ifndef _HPGUPPI_TIME_H
#define _HPGUPPI_TIME_H

#include <sys/time.h>

/* Compute PSRFITS-style integer MJD, integer second time of day, and
 * fractional second offset corresponding to the given timespec. */
int get_mjd_from_timespec(const struct timespec * ts,
    int *stt_imjd, int *stt_smjd, double *stt_offs);

/* Compute PSRFITS-style integer MJD, integer second time of day, and
 * fractional second offset corresponding to the given timeval. */
int get_mjd_from_timeval(const struct timeval * tv,
    int *stt_imjd, int *stt_smjd, double *stt_offs);

/* Return current time using PSRFITS-style integer MJD, integer
 * second time of day, and fractional second offset. */
int get_current_mjd(int *stt_imjd, int *stt_smjd, double *stt_offs);

/* Return Y, M, D, h, m, and s for an MJD */
int datetime_from_mjd(long double MJD, int *YYYY, int *MM, int *DD,
                      int *h, int *m, double *s);

/* Return the LST (in sec) for the GBT at a specific MJD (UTC) */
int get_current_lst(double mjd, int *lst_secs);

#endif
