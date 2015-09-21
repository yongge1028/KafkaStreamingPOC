curl -XPUT 'http://localhost:9200/twitter/' -d '{
    "settings" : {
        "number_of_shards" : 1,
        "number_of_replicas" : 0
    }
}'