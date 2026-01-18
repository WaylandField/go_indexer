#!/bin/bash
KAFKA_HOME=/Users/wayland/app/kafka_2.13-4.1.1

# 清理
rm -rf /tmp/kraft-combined-logs

# 生成集群ID（首次运行）
if [ ! -f /tmp/kafka-cluster-id ]; then
    $KAFKA_HOME/bin/kafka-storage.sh random-uuid > /tmp/kafka-cluster-id
fi

CLUSTER_ID=$(cat /tmp/kafka-cluster-id)

# 格式化存储
$KAFKA_HOME/bin/kafka-storage.sh format \
    -t $CLUSTER_ID \
    -c $KAFKA_HOME/config/kraft/server.properties

# 启动
$KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/kraft/server.properties