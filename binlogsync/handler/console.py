from __future__ import print_function, absolute_import, unicode_literals

import json
from binlogsync.handler.base import IEventHandler
from datetime import datetime
from pytz import timezone
from binlogsync.event.row_wrapper import InsertEventRow, UpdateEventRow, DeleteEventRow
from binlogsync.utils.json_encoder import make_json_encoder_default


class MysqlEvConsoleHandler(IEventHandler):
    def __init__(self,
                 ev_tz='Asia/Shanghai',
                 dt_col_tz='Asia/Shanghai',
                 indent=4):
        """
        :param ev_tz: timezone for serializing the event timestamp
        :param dt_col_tz: TIMESTAMP and DATETIME columns in MySQL also gives a naive datetime object,
                          timezone info is also need for serializing
        :param indent: json dump indent
        """
        self.ev_tz = timezone(ev_tz)
        self.dt_col_tz = timezone(dt_col_tz)
        self.indent = indent
        self.json_encoder_default = make_json_encoder_default(self.dt_col_tz)

    def to_dict(self, ev_id, ev_timestamp, schema, table, row):
        res = {
            'ev_id': ev_id,
            'ev_time': str(datetime.fromtimestamp(ev_timestamp, self.ev_tz)),
            'schema': schema,
            'table': table,
        }
        if isinstance(row, InsertEventRow):
            res['action'] = 'INSERT'
            res['new_values'] = row.new_values
        elif isinstance(row, UpdateEventRow):
            res['action'] = 'UPDATE'
            res['old_values'] = row.new_values
            res['new_values'] = row.new_values
        elif isinstance(row, DeleteEventRow):
            res['action'] = 'DELETE'
            res['old_values'] = row.old_values
        else:
            raise NotImplementedError

        return res

    def dump(self, ev_id, ev_timestamp, schema, table, affected_rows):
        for row in affected_rows:
            print(json.dumps(
                self.to_dict(ev_id, ev_timestamp, schema, table, row),
                indent=self.indent,
                default=self.json_encoder_default, ))

    def on_insert(self, ev_id, ev_timestamp, schema, table, affected_rows):
        self.dump(ev_id, ev_timestamp, schema, table, affected_rows)

    def on_update(self, ev_id, ev_timestamp, schema, table, affected_rows):
        self.dump(ev_id, ev_timestamp, schema, table, affected_rows)

    def on_delete(self, ev_id, ev_timestamp, schema, table, affected_rows):
        self.dump(ev_id, ev_timestamp, schema, table, affected_rows)
