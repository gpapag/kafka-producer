package com.mycompany.kafkaproducer;

import java.util.ArrayList;
import java.util.List;

public class KafkaProducerApp
{
    private static final int NUM_PARTITIONS = 4;

    private KafkaProducerApp()
    {
    }

    public static void main(String[] args)
    {
        if (args.length != 2) {
            System.out.println("Need to provide data source IP address and port");
            System.exit(1);
        }

        List<String> bootstrapServersList = new ArrayList<String>() {
            {
                add("198.168.0.1:9092");
                add("198.168.0.2:9092");
                add("198.168.0.3:9092");
                add("198.168.0.4:9092");
            }
        };

        String topic = "test";

        KProducer kafkaProducer = new KProducer(bootstrapServersList, topic, NUM_PARTITIONS);
        ConnManager connManager = new ConnManager(args[0], Integer.parseInt(args[1]), kafkaProducer);
        if (!connManager.isConnected()) {
            connManager.shutDown();
            System.err.println("Cannot connect to the data source");
            System.exit(1);
        }
        //connManager.shutDown();
    }
}
