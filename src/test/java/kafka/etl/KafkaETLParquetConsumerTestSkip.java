package kafka.etl;

import kafka.etl.deserialize.ClasspathAvroDeserializeService;
import kafka.etl.deserialize.ConsulAvroDeserializeService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.xml.DOMConfigurator;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class KafkaETLParquetConsumerTestSkip {

    private static Logger log;

    private String output = "/data/kafka-etl-parquet-test";

    @Before
    public void init() throws Exception {
        java.net.URL url = new KafkaETLParquetConsumerTestSkip().getClass().getResource("/log4j-test.xml");
        System.out.println("log4j url: " + url.toString());

        DOMConfigurator.configure(url);

        log = LoggerFactory.getLogger(KafkaETLParquetConsumerTestSkip.class);
    }

    @Test
    /**
     * Kafka ETL Consumer for Parquet with Avro Schema in Classpath.
     */
    public void runForClasspath() throws Exception
    {
        String brokers = System.getProperty("brokers", "localhost:9092");

        // kafka consumer properties.
        Properties kafkaConsumerProps = new Properties();
        kafkaConsumerProps.put("bootstrap.servers", brokers);
        kafkaConsumerProps.put("group.id", "kafka-etl-test-group");
        kafkaConsumerProps.put("enable.auto.commit", "false");
        kafkaConsumerProps.put("session.timeout.ms", "30000");
        kafkaConsumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        kafkaConsumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        // topic list which should consume.
        List<String> topics = new ArrayList<>();
        topics.add("item-view-event");

        // kafka consumer poll timeout.
        long pollTimeout = 1000;

        // parquet sink related properties.
        Map<String,String> parquetProps = new HashMap<>();
        parquetProps.put(KafkaETLParquetConsumer.CONF_HADOOP_CONF_DIR, "/etc/hadoop/conf");
        parquetProps.put(KafkaETLParquetConsumer.CONF_OUTPUT, output);
        parquetProps.put(KafkaETLParquetConsumer.CONF_DATE_FORMAT, "yyyy-MM-dd/HH/mm");
        parquetProps.put(KafkaETLParquetConsumer.CONF_INTERVAL_UNIT, KafkaETLParquetConsumer.IntervalUnit.MINUTE.name());
        parquetProps.put(KafkaETLParquetConsumer.CONF_INTERVAL, "1");

        // topic and avro schema classpath properties.
        // topic key must be in topic list.
        Properties topicAndPathProps = new Properties();
        topicAndPathProps.put("item-view-event", "/META-INF/avro/item-view-event.avsc");


        KafkaETLParquetConsumer kafkaETLConsumer =
                new KafkaETLParquetConsumer(kafkaConsumerProps, topics, pollTimeout, parquetProps, new ClasspathAvroDeserializeService(topicAndPathProps));
        kafkaETLConsumer.run();

        Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new ShutdownHookThread(kafkaETLConsumer, mainThread));
    }

    private static class ShutdownHookThread extends Thread
    {
        private KafkaETLParquetConsumer consumer;
        private Thread mainThread;

        public ShutdownHookThread(KafkaETLParquetConsumer consumer, Thread mainThread)
        {
            this.consumer = consumer;
            this.mainThread = mainThread;
        }

        public void run() {
            log.info("Starting exit...");
            // Note that shutdownhook runs in a separate thread, so the only thing we can safely do to a consumer is wake it up
            consumer.stop();
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    /**
     * Kafka ETL Consumer for Parquet with Avro Schema in Consul.
     */
    public void runForConsul() throws Exception
    {
        String brokers = System.getProperty("brokers", "localhost:9092");

        // kafka consumer properties.
        Properties kafkaConsumerProps = new Properties();
        kafkaConsumerProps.put("bootstrap.servers", brokers);
        kafkaConsumerProps.put("group.id", "kafka-etl-test-group");
        kafkaConsumerProps.put("enable.auto.commit", "false");
        kafkaConsumerProps.put("session.timeout.ms", "30000");
        kafkaConsumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        kafkaConsumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        // topic list which should consume.
        List<String> topics = new ArrayList<>();
        topics.add("item-view-event");

        // kafka consumer poll timeout.
        long pollTimeout = 1000;

        // parquet sink related properties.
        Map<String,String> parquetProps = new HashMap<>();
        parquetProps.put(KafkaETLParquetConsumer.CONF_HADOOP_CONF_DIR, "/etc/hadoop/conf");
        parquetProps.put(KafkaETLParquetConsumer.CONF_OUTPUT, output);
        parquetProps.put(KafkaETLParquetConsumer.CONF_DATE_FORMAT, "yyyy-MM-dd/HH/mm");
        parquetProps.put(KafkaETLParquetConsumer.CONF_INTERVAL_UNIT, KafkaETLParquetConsumer.IntervalUnit.MINUTE.name());
        parquetProps.put(KafkaETLParquetConsumer.CONF_INTERVAL, "1");

        // topic and avro schema consul key path properties.
        // topic key must be in topic list.
        Properties topicAndPathProps = new Properties();
        topicAndPathProps.put("item-view-event", "avro-schemas/item-view-event");

        // consul agent host and port.
        String agentHost = "localhost";
        int agentPort = 8500;


        KafkaETLParquetConsumer kafkaETLConsumer =
                new KafkaETLParquetConsumer(kafkaConsumerProps, topics, pollTimeout, parquetProps, new ConsulAvroDeserializeService(topicAndPathProps, agentHost, agentPort));
        kafkaETLConsumer.run();
    }
}
