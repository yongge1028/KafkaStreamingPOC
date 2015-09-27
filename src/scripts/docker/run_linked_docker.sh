docker run -d -p 9200:9200 -p 9300:9300 itzg/elasticsearch:1shard_network
docker run --name some-kibana -e ELASTICSEARCH_URL=http://192.168.99.100:9200 -p 5601:5601 -d kibana