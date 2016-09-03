from __future__ import absolute_import, unicode_literals

import logging

_logger = logging.getLogger('binlogsync')
_logger.setLevel(logging.INFO)

_ch_handler = logging.StreamHandler()
_ch_handler.setLevel(logging.DEBUG)
_ch_handler.setFormatter(logging.Formatter(
    '%(levelname)s %(asctime)s %(message)s'))

_logger.addHandler(_ch_handler)


def get_logger():
    global _logger
    return _logger
