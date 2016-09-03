from __future__ import absolute_import, print_function, unicode_literals

from datetime import date, datetime


def make_json_encoder_default(tz):
    def default(o):
        if isinstance(o, datetime):
            return str(tz.localize(o.replace(microsecond=0)))
        elif isinstance(o, date):
            return str(o)
        else:
            return repr(o)

    return default
