from __future__ import absolute_import, unicode_literals

import sys
import os
import signal
import traceback
from binlogsync.event.stream import MysqlEventStream
from binlogsync.event.processor import MysqlEventProcessor
from binlogsync.handler.base import IEventHandler
from binlogsync.log import get_logger


class BinlogSyncWrapper(object):
    def __init__(self,
                 mysql_conn_settings,
                 replication_server_id,
                 event_handler,
                 dump_file_path,
                 dump_interval=10,
                 table_filters=None):
        """
        :param mysql_conn_settings: {
                                        "host":MYSQL_MASTER_SERVER_HOST,
                                        "port":MYSQL_MASTER_PORT,
                                        "user":USERNAME,
                                        "passwd":PASSWORD,
                                    }
                                    make sure the account have appropriate privileges.
                                    http://dev.mysql.com/doc/refman/en/replication-howto-repuser.html

        :param replication_server_id: the daemon behaves like a MySQL replication slave server,
                                      you need to assign a unique server id(an integer) for it,
                                      remember do NOT conflict with other replication slave server
                                      http://dev.mysql.com/doc/refman/en/replication-howto-slavebaseconfig.html
        :param event_handler: the event handler
                              must be an instance derived from BinlogSync.handler.base.IEventHandler
        :param dump_file_path: the daemon need to dump currently read binlog file and offset to a file
                               on a regular interval or when exceptions thrown
        :param dump_interval: dump every <dump_interval> seconds
        :param table_filters: a dict of lists
        :return:
        """
        assert (isinstance(mysql_conn_settings, dict))
        assert (all([
            'host' in mysql_conn_settings,
            'port' in mysql_conn_settings,
            'user' in mysql_conn_settings,
            'passwd' in mysql_conn_settings,
        ]))
        assert (isinstance(event_handler, IEventHandler))

        if os.path.exists(dump_file_path):
            event_stream = MysqlEventStream.loadf(
                path=dump_file_path,
                mysql_settings=mysql_conn_settings,
                table_filters=table_filters)
        else:
            event_stream = MysqlEventStream(mysql_settings=mysql_conn_settings,
                                            server_id=replication_server_id,
                                            dump_file_path=dump_file_path)
            # make sure we have write access to the dump file
            event_stream.dumpf()

        self.event_processor = MysqlEventProcessor(
            ev_stream=event_stream,
            ev_handler=event_handler,
            dump_interval=dump_interval, )

        signal.signal(signal.SIGTERM, self.sig_handler)
        signal.signal(signal.SIGINT, self.sig_handler)

    def close(self):
        self.event_processor.ev_stream.dumpf()
        self.event_processor.ev_stream.close()
        self.event_processor.ev_handler.close()

    def sig_handler(self, sig, frame):
        logger = get_logger()
        logger.warning('Signal SIGTERM/SIGINT received, exit.')
        self.close()
        sys.exit(sig + 128)

    def run(self):
        try:
            while True:
                self.event_processor.run()
        except Exception as e:
            # something unexpected happened
            # just leaving a traceback log and exit
            logger = get_logger()
            tb = traceback.format_exc()
            logger.error('Exception caught with traceback info:\n' + tb)
            self.close()
            sys.exit(1)
