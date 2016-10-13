
package com.frontier45.flume.sink.elasticsearch2;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;

import com.frontier45.flume.sink.elasticsearch2.client.ElasticSearchClient;
import com.frontier45.flume.sink.elasticsearch2.client.ElasticSearchClientFactory;

public class test {
	static ElasticSearchClient client = null;

	private static IndexNameBuilder indexNameBuilder;
	
	private static ElasticSearchEventSerializer eventSerializer;
	private static ElasticSearchIndexRequestBuilderFactory indexRequestFactory;
	
	public static void main(String[] args) throws Exception {

		
		Context serializerContext = new Context();
		serializerContext.put("test",ElasticSearchSinkConstants.SERIALIZER_PREFIX);
		
		Class<? extends Configurable> clazz = (Class<? extends Configurable>) Class.forName("com.frontier45.flume.sink.elasticsearch2.ElasticSearchDynamicSerializer");
		Configurable serializer = clazz.newInstance();
		
		eventSerializer = (ElasticSearchEventSerializer) serializer;
		eventSerializer.configure(serializerContext);
		

		
		ElasticSearchClientFactory clientFactory = new ElasticSearchClientFactory();
		String[] strhost = {"192.168.10.132"};
		String[] searchGuardStrs = null;
		try {
			client = clientFactory.getClient("transport",strhost, "bds-elasticsearch-cluster", eventSerializer,
					indexRequestFactory,searchGuardStrs);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			
			e.printStackTrace();
		}
		Event event = EventBuilder.withBody("".getBytes());
		Class<? extends IndexNameBuilder> clazz2 = (Class<? extends IndexNameBuilder>) Class
				.forName("com.frontier45.flume.sink.elasticsearch2.TimeBasedIndexNameBuilder");
		indexNameBuilder = clazz2.newInstance();
		
		Context indexnameBuilderContext = new Context();
		indexnameBuilderContext.put(ElasticSearchSinkConstants.INDEX_NAME, "我是丹丹");
		
		indexNameBuilder.configure(indexnameBuilderContext);
		
		
		
		client.addEvent(event, indexNameBuilder, "log", 5);
		

		client.execute();
	}

}
