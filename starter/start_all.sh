cd
cd kafka_2.12-2.3.0/
bin/zookeeper-server-start.sh config/zookeeper.properties &
cd
cd kafka_2.12-2.3.0/
bin/kafka-server-start.sh config/server.properties &
cd
cassandra -f &