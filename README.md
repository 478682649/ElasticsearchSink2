# flume-elasticsearch-sink2

Add search guard ssl plugin 

## Current Version

* Development: 0.0.1-SNAPSHOT

## Current Supported Features
	eg:
bde.sinks.k_notice.type = com.frontier45.flume.sink.elasticsearch2.ElasticSearchSink
bde.sinks.k_notice.hostNames = 127.0.0.1:9300
bde.sinks.k_notice.indexName = name here
bde.sinks.k_notice.indexType = log 
bde.sinks.k_notice.clusterName = bds-elasticsearch-cluster
bde.sinks.k_notice.batchSize = 100
bde.sinks.k_notice.ttl = 5 
bde.sinks.k_notice.serializer = com.frontier45.flume.sink.elasticsearch2.ElasticSearchDynamicSerializer
bde.sinks.k_notice.indexNameBuilder = com.frontier45.flume.sink.elasticsearch2.TimeBasedIndexNameBuilder
bde.sinks.k_notice.keystorePath = /Path/admin-keystore.jks
bde.sinks.k_notice.keystorePass = changeit
bde.sinks.k_notice.truststorePath = /Path/truststore.jks
bde.sinks.k_notice.truststorePass = changeit


