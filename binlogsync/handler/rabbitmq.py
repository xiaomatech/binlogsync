from __future__ import print_function, absolute_import, unicode_literals

import json
from binlogsync.handler.base import IEventHandler
from datetime import datetime
from pytz import timezone
from binlogsync.event.row_wrapper import InsertEventRow, UpdateEventRow, DeleteEventRow
from binlogsync.utils.json_encoder import make_json_encoder_default
import pika


class MysqlEvRabbitmqHandler(IEventHandler):
    def __init__(self,
                 rabbitmq_config=None,
                 ev_tz='Asia/Shanghai',
                 dt_col_tz='Asia/Shanghai',
                 topic_func=None,
                 split_row=False):
        """

        :param ev_tz: timezone for serializing the event timestamp
        :param dt_col_tz: TIMESTAMP and DATETIME columns in MySQL also gives a naive datetime object,
                          timezone info is also need for serializing
        :param rabbitmq_config:
        :param topic_func: the function must receive 2 params(schema, table) and return a string
        :param split_row: if set to True, handler will split affected rows into messages,
                          each message contains only one row with "msg_key":"<ev_id>#<row_index>"
                          otherwise, handler just send one message for one event containing all affected rows,
                          each message has the key "ev_id":"<ev_id>" with "affected_rows[]"
        """
        try:
            self.rabbitmq_config = rabbitmq_config
            self.rmq_conn = pika.BlockingConnection(pika.ConnectionParameters(rabbitmq_config))
            self.channel = self.rmq_conn.channel()
            # 定义交换机
            self.channel.exchange_declare(exchange=self.rabbitmq_config.get('exchange'), type=self.rabbitmq_config.get('exchange_type'))
        except :
            raise Exception('Invalid args for create kafka handler instance')

        if callable(topic_func):
            self.topic_func = topic_func

        self.split_row = split_row
        self.ev_tz = timezone(ev_tz)
        self.dt_col_tz = timezone(dt_col_tz)
        self.json_encoder_default = make_json_encoder_default(self.dt_col_tz)

    @classmethod
    def gen_msg_key(cls, ev_id, row_index=None):
        if row_index:
            return '#'.join([ev_id, str(row_index)])
        else:
            return ev_id

    def to_dict(self, ev_id, ev_timestamp, schema, table, row, msg_key=None):
        res = {
            'ev_id': ev_id,
            'ev_time': str(datetime.fromtimestamp(ev_timestamp, self.ev_tz)),
            'schema': schema,
            'table': table,
        }
        if msg_key:
            res['msg_key'] = msg_key

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

    def send_msgs(self, ev_id, ev_timestamp, schema, table, affected_rows):
        if callable(self.topic_func):
            topic = self.topic_func(schema=schema, table=table)
        else:
            topic = 'sync_' + schema + '_' + table
        self.channel.queue_declare(queue=topic)
        if self.split_row:
            msg_list = []
            for row_index, row in enumerate(affected_rows):
                msg = self.to_dict(ev_id, ev_timestamp, schema, table, row,
                                   self.gen_msg_key(ev_id, row_index))
                msg_list.append(json.dumps(msg,
                                           default=self.json_encoder_default))

                #将消息发送到交换机
                self.channel.basic_publish(exchange=self.rabbitmq_config.get('exchange'),
                                            routing_key=self.rabbitmq_config.get('routing_key_prefix'),
                                            body=msg_list)
        else:
            row_list = [self.to_dict(ev_id, ev_timestamp, schema, table, row)
                        for row in affected_rows]
            msg_dict = {
                'ev_id': ev_id,
                'ev_time':
                str(datetime.fromtimestamp(ev_timestamp, self.ev_tz)),
                'schema': schema,
                'table': table,
                'affected_rows': row_list,
            }
            msg = json.dumps(msg_dict, default=self.json_encoder_default)
            #将消息发送到交换机
            self.channel.basic_publish(exchange=self.rabbitmq_config.get('exchange'),
                                       routing_key=self.rabbitmq_config.get('routing_key_prefix')+self.gen_msg_key(ev_id),
                                       body=msg)

    def on_insert(self, ev_id, ev_timestamp, schema, table, affected_rows):
        self.send_msgs(ev_id, ev_timestamp, schema, table, affected_rows)

    def on_update(self, ev_id, ev_timestamp, schema, table, affected_rows):
        self.send_msgs(ev_id, ev_timestamp, schema, table, affected_rows)

    def on_delete(self, ev_id, ev_timestamp, schema, table, affected_rows):
        self.send_msgs(ev_id, ev_timestamp, schema, table, affected_rows)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.channel.close()
