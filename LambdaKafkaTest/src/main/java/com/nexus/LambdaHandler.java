package com.nexus;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.fasterxml.jackson.databind.ObjectMapper;

public class LambdaHandler implements RequestHandler<S3Event, String> {

	final AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
	// private Map<String, Mapping> mappingdao = new HashMap<>();
	private List<String> jsondao = new ArrayList<>();

	public String handleRequest(S3Event input, Context context) {
		
		S3EventNotificationRecord record = input.getRecords().get(0);
		String srcBucket = record.getS3().getBucket().getName();
		String eventName = record.getEventName();
		context.getLogger().log("Event is generated with : " + srcBucket + " -EventName- " + eventName);
		
		S3Object s3object = s3.getObject("carefirst-nexus-project", "lambdaMapping.csv");
		S3ObjectInputStream file = s3object.getObjectContent();
		
		String line = "";
		BufferedReader br = null;
		try {
			br = new BufferedReader(new InputStreamReader(file));
			while ((line = br.readLine()) != null) {
				String[] attribute = line.split(",");
				Mapping map = new Mapping(attribute[0], attribute[1], attribute[2], attribute[3]);
				ObjectMapper mapper = new ObjectMapper();
				String jsonString = mapper.writeValueAsString(map);
				// mappingdao.put(map.getId(), map);
				jsondao.add(jsonString);
			}
		} catch (IOException e) {
			context.getLogger().log(e.getMessage());
		} finally {
			try {
				file.close();
				br.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		/*
		 * String KAFKA_SERVER_URL = "157.33.219.32"; String KAFKA_SERVER_PORT = "9092";
		 * Properties properties = new Properties(); properties.put("bootstrap.servers",
		 * KAFKA_SERVER_URL + ":" + KAFKA_SERVER_PORT); properties.put("client.id",
		 * "DemoProducer"); properties.put("key.serializer",
		 * "org.apache.kafka.common.serialization.StringSerializer");
		 * properties.put("value.serializer",
		 * "org.apache.kafka.common.serialization.StringSerializer"); KafkaProducer
		 * producer = new KafkaProducer(properties); producer.send(new
		 * ProducerRecord("topicL", "lambdakey", jsondao));
		 */

		context.getLogger().log("All JSON Data is  : " + jsondao);
		// context.getLogger().log("All Data is : "+mappingdao.values());

		return eventName;
	}
}
