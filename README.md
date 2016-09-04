# binlogsync
通过 mysql binlog event 把sql变更实时推送到各系统(cache/redis,search/elasticsearch,queue/kafka/rabbitmq)

#使用
- 安装依赖 pip install -r requirements.txt
- 修改example.py中的配置
- 启动 nohup python example.py &
