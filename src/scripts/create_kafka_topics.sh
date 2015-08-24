./kafka-topics.sh --create --partitions 3 --replication-factor 3 --topic netflow-input --zookeeper vm-cluster-node1:2181
./kafka-topics.sh --describe --topic netflow-input --zookeeper vm-cluster-node1:2181
./kafka-topics.sh --list --zookeeper vm-cluster-node1:2181