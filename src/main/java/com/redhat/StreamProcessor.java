package com.redhat;

import io.quarkus.runtime.StartupEvent;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.Map;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import java.time.Duration;

@ApplicationScoped
@Path("/data")
public class StreamProcessor {


    @ConfigProperty(name = "bootstrap.servers")
    String bootstrapServers;


    void onStart(@Observes StartupEvent ev) {
        

    }

    @GET
    @Path("/get")
    @Produces(MediaType.APPLICATION_JSON)
    public Object[] getData() {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        // Create a Kafka consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        TopicPartition partition0 = new TopicPartition("sensor-data", 0); // Specify topic and partition
        consumer.assign(Arrays.asList(partition0));

        // Define the time window to fetch messages from (last minute)
        long oneMinuteAgo = System.currentTimeMillis() - 60000; // Timestamp for one minute ago

        // Wait until the consumer is assigned partitions
        consumer.poll(Duration.ofMillis(100)); // You may need to adjust this duration
        consumer.assignment().forEach(partition -> {
            // For each partition, find the offset at the timestamp
            Map<TopicPartition, Long> timestampToSearch = Map.of(partition, oneMinuteAgo);
            Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes = consumer.offsetsForTimes(timestampToSearch);
            if (offsetsForTimes != null && offsetsForTimes.get(partition) != null) {
                long startOffset = offsetsForTimes.get(partition).offset();
                consumer.seek(partition, startOffset);
            }
        });

        // Array to store messages
        ArrayList<String> messages = new ArrayList<>();

        try {
            // Poll for new data from the topic
            boolean keepPolling = true;
            while (keepPolling) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    messages.add(record.value()); // Add the message value to the array
                    System.out.println("Received message: (key: " + record.key() + ", value: " + record.value() + ")");
                }
                if (records.isEmpty()) {
                    keepPolling = false; // Stop polling if no more messages are fetched
                }
            }
        } finally {
            consumer.close(); // Always close the consumer
        }

        // Optionally, print or return your array of messages
        messages.forEach(System.out::println);

        return messages.toArray();
    }

    @GET
    @Path("/print")
    @Produces(MediaType.APPLICATION_JSON)
    public void printAllData() {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers); // Set your Kafka broker address
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"); // Key deserializer
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"); // Value deserializer

        // Create a Kafka consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        TopicPartition partition0 = new TopicPartition("sensor-data", 0); // Specify topic and partition
        consumer.assign(Arrays.asList(partition0));


        // Subscribe to the "sensor-data" topic
        //consumer.subscribe(Collections.singletonList("sensor-data"));
        System.out.println("Subscribed");

        // Poll for new data and print it
        try {
            for(int i=0; i<100; i++){
                System.out.println("Reading next record");
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("offset = %d, key = %s, value = %s%n, timestamp = %d", record.offset(), record.key(), record.value(), record.timestamp());
                }
            }
        } finally {
            consumer.close(); // Close the consumer
        }
    }
}
