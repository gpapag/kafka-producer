package com.mycompany.kafkaproducer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.List;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class KProducer
{
    private static final Logger logger = LoggerFactory.getLogger(KProducer.class);

    private final int numPartitions;
    private final String topic;
    private final KafkaProducer<String, byte[]> producer;

    public KProducer(List<String> bootstrapServersList, String topic, int numPartitions)
    {
        requireNonNull(bootstrapServersList, "bootstrapServersList is null");
        checkArgument(numPartitions > 0, "numPartitions is negative");

        Properties properties = new Properties() {
            {
                put("bootstrap.servers", String.join(",", bootstrapServersList));
                put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
                put("acks", "all");
            }
        };
        this.numPartitions = numPartitions;
        this.topic = topic;

        producer = new KafkaProducer<>(properties);
    }

    public int getNumPartitions()
    {
        return numPartitions;
    }

    public boolean produceBytes(Integer partition, String key, byte[] value)
    {
        return produceBytes(topic, partition, key, value);
    }

    public boolean produceBytes(String topic, Integer partition, String key, byte[] value)
    {
        boolean isSuccessful = true;

        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        Long recordTimestamp = timestamp.getTime();
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, partition, recordTimestamp, key, value);

        try {
            producer.send(record).get();
            logger.debug("Sent Topic[{}] Partition[{}] Timestamp[{}] Key[{}]", topic, partition, recordTimestamp, key);
        }
        catch (Exception e) {
            logger.error(e.getMessage());
            isSuccessful = false;
        }

        return isSuccessful;
    }

    public void shutDown()
    {
        producer.close();
    }
}
