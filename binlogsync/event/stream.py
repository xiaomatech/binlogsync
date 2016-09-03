from __future__ import absolute_import, unicode_literals

import json
import io
from pymysqlreplication import BinLogStreamReader


class MysqlEventStream(object):
    def __init__(self,
                 mysql_settings,
                 server_id,
                 dump_file_path,
                 log_file=None,
                 log_pos=None,
                 gtid_set=None,
                 table_filters=None):
        # TODO: gtid mode support
        # https://dev.mysql.com/doc/refman/en/replication-gtids.html
        # TODO: wild chars in table_filters
        self.mysql_settings = mysql_settings
        self.server_id = server_id
        self.log_file = log_file
        self.log_pos = log_pos
        self.dump_file_path = dump_file_path

        if table_filters:
            self.table_filters = {schema: frozenset(tables)
                                  for schema, tables in table_filters.items()}
            only_schemas = [schema for schema in table_filters]
        else:
            self.table_filters = None
            only_schemas = None

        self.binlog_stream_reader = BinLogStreamReader(
            connection_settings=self.mysql_settings,
            server_id=self.server_id,
            log_file=self.log_file,
            log_pos=self.log_pos,
            resume_stream=True,
            blocking=False,
            freeze_schema=True,
            only_schemas=only_schemas, )

    def __iter__(self):
        for ev in self.binlog_stream_reader:
            yield (self.binlog_stream_reader.log_file,
                   self.binlog_stream_reader.log_pos,
                   ev, )

    def process_commit(self, new_log_file, new_log_pos):
        self.log_file = new_log_file
        self.log_pos = new_log_pos

    def close(self):
        """close connection to MySQL
        """
        self.binlog_stream_reader.close()

    def dumpf(self):
        """dump currently read binlog file and offset to a file
        """
        data = {
            'server_id': self.server_id,
            'log_file': self.log_file,
            'log_pos': self.log_pos,
        }
        data_str = json.dumps(data)
        with io.open(self.dump_file_path, 'w', encoding='utf-8') as f:
            f.write(data_str)

    @classmethod
    def loadf(cls, path, mysql_settings, table_filters=None):
        """construct from dump file
        """
        with io.open(path, 'r', encoding='utf-8') as f:
            data_str = f.read()

        data = json.loads(data_str)

        ev_stream = cls(mysql_settings=mysql_settings,
                        server_id=data['server_id'],
                        log_file=data['log_file'],
                        log_pos=data['log_pos'],
                        dump_file_path=path,
                        table_filters=table_filters, )

        return ev_stream
