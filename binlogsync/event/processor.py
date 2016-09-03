from __future__ import absolute_import, unicode_literals

import time
from pymysqlreplication.row_event import WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent, RowsEvent
from binlogsync.event.stream import MysqlEventStream
from binlogsync.handler.base import IEventHandler
from binlogsync.log import get_logger


class MysqlEventProcessor(object):
    def __init__(self, ev_stream, ev_handler, dump_interval=10):
        assert (isinstance(ev_stream, MysqlEventStream))
        assert (isinstance(ev_handler, IEventHandler))
        self.ev_stream = ev_stream
        self.ev_handler = ev_handler
        self.dump_interval = dump_interval
        self.last_dump_time = time.time()

        self.sleep_interval = 1

    def reset_sleep_interval(self):
        self.sleep_interval = 1

    def sleep_by_eof(self):
        time.sleep(self.sleep_interval)
        if self.sleep_interval < 16:
            self.sleep_interval *= 2

    @classmethod
    def gen_ev_id(cls, log_file, log_pos):
        return '#'.join([log_file, str(log_pos)])

    def run(self):
        logger = get_logger()
        for new_log_file, new_log_pos, ev in self.ev_stream:
            # check if we need to process this event
            # i.e. not filtered by table filters and is instance of RowsEvent
            need_process = True
            if not isinstance(ev, RowsEvent):
                need_process = False
            elif self.ev_stream.table_filters:
                schema = ev.schema
                table = ev.table
                if (schema not in self.ev_stream.table_filters) or (
                        table not in self.ev_stream.table_filters[schema]):
                    need_process = False

            if need_process:
                ev_id = self.gen_ev_id(log_file=new_log_file,
                                       log_pos=new_log_pos)
                self.reset_sleep_interval()

                if isinstance(ev, WriteRowsEvent):
                    self.ev_handler.on_insert_raw(ev_id, ev)
                elif isinstance(ev, UpdateRowsEvent):
                    self.ev_handler.on_update_raw(ev_id, ev)
                elif isinstance(ev, DeleteRowsEvent):
                    self.ev_handler.on_delete_raw(ev_id, ev)
                else:
                    # currently, we don't have implementations for these events
                    logger.debug('No handler for event %s with event type %s.',
                                 ev_id, ev.__class__.__name__)
                    pass

            # commit log_file and log_pos change to event stream reader
            self.ev_stream.process_commit(new_log_file, new_log_pos)
            # check if dump is needed
            now_time = time.time()
            if now_time - self.last_dump_time > self.dump_interval:
                self.ev_stream.dumpf()
                self.last_dump_time = now_time

        logger.debug(
            'EOF packet received. The processor has proceeded to the last record of binlog.')
        self.sleep_by_eof()
