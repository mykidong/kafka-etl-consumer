package kafka.etl;

import kafka.etl.deserialize.ClasspathAvroDeserializeService;
import org.apache.log4j.xml.DOMConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by mykidong on 2016-08-03.
 */
public class KafkaETLConsumerMain {

    private static Logger log;

    public static void main(String[] args)
    {
        java.net.URL url = new KafkaETLParquetConsumerTestSkip().getClass().getResource("/log4j-test.xml");
        System.out.println("log4j url: " + url.toString());

        DOMConfigurator.configure(url);

        log = LoggerFactory.getLogger(KafkaETLConsumerMain.class);

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

        String output = "/data/kafka-etl-parquet-test";

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
    }
}
