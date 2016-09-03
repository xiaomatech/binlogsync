from __future__ import absolute_import, unicode_literals

import time
from pymysqlreplication.row_event import WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent, RowsEvent
from binlogsync.event.stream import MysqlEventStream
from binlogsync.handler.base import IEventHandler
from binlogsync.log import get_logger
from threading import Thread
import gevent
from gevent import monkey
monkey.patch_all()

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
        gevent.sleep(self.sleep_interval)
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
                    #self.ev_handler.on_insert_raw(ev_id, ev)
                    insert_thread = Thread(target=self.ev_handler.on_insert_raw,name='on_insert_raw%s'%ev_id,args=(ev_id,ev))
                    insert_thread.start()
                elif isinstance(ev, UpdateRowsEvent):
                    #self.ev_handler.on_update_raw(ev_id, ev)
                    update_thread = Thread(target=self.ev_handler.on_update_raw,name='on_update_raw%s'%ev_id,args=(ev_id,ev))
                    update_thread.start()
                elif isinstance(ev, DeleteRowsEvent):
                    #self.ev_handler.on_delete_raw(ev_id, ev)
                    delete_thread = Thread(target=self.ev_handler.on_delete_raw,name='on_delete_raw%s'%ev_id,args=(ev_id,ev))
                    delete_thread.start()
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
