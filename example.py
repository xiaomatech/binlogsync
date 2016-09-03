# -*- coding:utf8 -*-
table_filters = {
    'db1': ['table_a','table_b'],  #只同步db1中的表table_a,table_b表
}

mysql_config = {
    'host': '127.0.0.1',
    'port': 3306,
    'user': 'root',
    'passwd': '',
}

kafka_config = ["127.0.0.2:9092", "127.0.0.3:9092"]

es_config = {'host':'','port':'','excludes_fields':['secret','password']}

redis_config = {'host': 'localhost', 'port': 6379, 'db': 0}

from binlogsync.daemon import BinlogSyncWrapper

#kafka
from binlogsync.handler.kafka import MysqlEvKafkaHandler

kafka_hdlr = MysqlEvKafkaHandler(hosts=kafka_config)

kafka_sync = BinlogSyncWrapper(mysql_conn_settings=mysql_config,
                         replication_server_id=11,
                         event_handler=kafka_hdlr,
                         dump_file_path='/tmp/binlogsync_kafka.dmp',
                         table_filters=table_filters)
kafka_sync.run()



#es
from binlogsync.handler.elasticsearch import MysqlEvElasticsearchHandler

es_hdlr = MysqlEvElasticsearchHandler(config=es_config)

es_sync = BinlogSyncWrapper(mysql_conn_settings=mysql_config,
                               replication_server_id=11,
                               event_handler=es_hdlr,
                               dump_file_path='/tmp/binlogsync_es.dmp',
                               table_filters=table_filters)
es_sync.run()


#redis

from binlogsync.handler.redis import MysqlEvRedisHandler
redis_hdlr = MysqlEvRedisHandler(redis_config=redis_config)

redis_sync = BinlogSyncWrapper(mysql_conn_settings=mysql_config,
                            replication_server_id=11,
                            event_handler=redis_hdlr,
                            dump_file_path='/tmp/binlogsync_redis.dmp',
                            table_filters=table_filters)
redis_sync.run()



# console

# from binlogsync.handler.console import MysqlEvConsoleHandler
#
# console_hdlr = MysqlEvConsoleHandler()
#
# console_sync = BinlogSyncWrapper(mysql_conn_settings=mysql_config,
#                          replication_server_id=10,
#                          event_handler=console_hdlr,
#                          dump_file_path='/tmp/binlogsync.dmp',
#                          table_filters=table_filters)
# console_sync.run()
