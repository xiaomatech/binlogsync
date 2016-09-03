from __future__ import print_function, absolute_import, unicode_literals


def tzlc(dt, tz, truncate_to_sec=True):
    if dt is None:
        return None
    if truncate_to_sec:
        dt = dt.replace(microsecond=0)
    return tz.localize(dt)
