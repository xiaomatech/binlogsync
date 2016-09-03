from __future__ import print_function, absolute_import, unicode_literals

from binlogsync.handler.base import IEventHandler
import redis


class MysqlEvRedisHandler(IEventHandler):
    def __init__(self, redis_config=None, redis_instance=None):

        if redis_instance:
            self.redis = redis_instance
        else:
            try:
                pool = redis.ConnectionPool(**redis_config)
                self.redis = redis.Redis(connection_pool=pool)
            except Exception as e:
                raise Exception(
                    'Invalid args for create kafka handler instance')

    def on_delete_raw(self, ev_id, ev):
        for row in ev.rows:
            pk = ev.primary_key
            table = ev.table
            schema = ev.schema
            prefix = "%s:%s:" % (schema, table)
            yield self.redis.delete(prefix + str(row['values'][pk]))

    def on_update_raw(self, ev_id, ev):
        for row in ev.rows:
            pk = ev.primary_key
            table = ev.table
            schema = ev.schema
            prefix = "%s:%s:" % (schema, table)
            yield self.redis.hmset(prefix + str(row['values'][pk]),
                                   row['values'])

    def on_insert_raw(self, ev_id, ev):
        for row in ev.rows:
            pk = ev.primary_key
            table = ev.table
            schema = ev.schema
            prefix = "%s:%s:" % (schema, table)
            yield self.redis.hmset(prefix + str(row['values'][pk]),
                                   row['values'])
