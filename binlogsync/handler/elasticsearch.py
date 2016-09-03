from __future__ import print_function, absolute_import, unicode_literals

from binlogsync.handler.base import IEventHandler
from datetime import date, datetime
from pyelasticsearch import ElasticSearch


class MysqlEvElasticsearchHandler(IEventHandler):
    def __init__(self, config=None, es_instance=None):
        if es_instance:
            self.es = es_instance
        else:
            self.config = config
            self.excludes_fields = self.config['excludes_fields']
            self.es = ElasticSearch('http://{host}:{port}/'.format(
                host=self.config['host'],
                port=self.config['port']))

    def _format(self, dat):
        for k, v in dat.items():
            if isinstance(v, datetime):
                dat[k] = v.strftime('%Y-%m-%d %H:%M:%S')
            elif isinstance(v, date):
                dat[k] = v.strftime('%Y-%m-%d')
            if k in self.excludes_fields:
                del dat[k]
        return dat

    def on_delete_raw(self, ev_id, ev):
        for row in ev.rows:
            pk = ev.primary_key
            table = ev.table
            schema = ev.schema
            yield self.es.index_op(
                self._format(row['values']),
                doc_type=table,
                index=schema,
                id=row['values'][pk])

    def on_update_raw(self, ev_id, ev):
        for row in ev.rows:
            pk = ev.primary_key
            table = ev.table
            schema = ev.schema
            yield self.es.update_op(
                self._format(row['after_values']),
                doc_type=table,
                index=schema,
                id=row['after_values'][pk])

    def on_insert_raw(self, ev_id, ev):
        for row in ev.rows:
            pk = ev.primary_key
            table = ev.table
            schema = ev.schema
            yield self.es.delete_op(doc_type=table,
                                    index=schema,
                                    id=row['values'][pk])
