package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoForDifferentPartitions {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoForDifferentPartitions.class);

    public static void main(String[] args) throws InterruptedException {
        log.info("Hello World");

        //Create properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","127.0.0.1:29092");

        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        //Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int j = 0; j < 3; j++) {

            for (int i = 0; i < 10; i++) {

                //Create a Producer Record
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo3_java", (" hello world " + i));

                //Send data with Callback
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e == null) {
                            log.info("Received new metadata : Topic: " + recordMetadata.topic() +
                                    " partition: " + recordMetadata.partition() +
                                    " Offset : " + recordMetadata.offset() +
                                    " TimeStamp: " + recordMetadata.timestamp()
                            );
                        } else {
                            log.error("Error with exception " + e);
                        }
                    }
                });

            }

            //Add below sleep so that the events will go to different partitions. w/o this all might go to same partition
            Thread.sleep(5000);

        }


        //flush and close the producer. Tell the producer to send all data and block until done - synchronous call
        producer.flush();

        producer.close();

    }
}