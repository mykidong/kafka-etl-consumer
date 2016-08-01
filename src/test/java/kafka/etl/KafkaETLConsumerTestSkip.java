package kafka.etl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.xml.DOMConfigurator;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class KafkaETLConsumerTestSkip {

    private static Logger log;

    private static final String AVRO_SCHEMA_CONTEXT = "classpath*:/META-INF/spring/avro-schema/*-context.xml";

    private String output = "/data/kafka-etl-test";

    @Before
    public void init() throws Exception {
        java.net.URL url = new KafkaETLConsumerTestSkip().getClass().getResource("/log4j-test.xml");
        System.out.println("log4j url: " + url.toString());

        DOMConfigurator.configure(url);

        log = LoggerFactory.getLogger(KafkaETLConsumerTestSkip.class);

        String hadoopConfDir = "/etc/hadoop/conf";

        Configuration configuration = new Configuration();
        configuration.addResource(new Path(hadoopConfDir + "/core-site.xml"));
        configuration.addResource(new Path(hadoopConfDir + "/hdfs-site.xml"));

        FileSystem fs = FileSystem.get(configuration);
        fs.delete(new Path(output), true);
    }

    @Test
    public void run() throws Exception
    {
        String brokers = System.getProperty("brokers", "ieiot-q01-dev.gt.com:6667,ieiot-q02-dev.gt.com:6667");

        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("group.id", "kafka-etl-test");
        props.put("enable.auto.commit", "false");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        List<String> topics = new ArrayList<>();
        topics.add("item-view-event");

        long timeout = 1000;

        Map<String,String> parquetProps = new HashMap<>();
        parquetProps.put(KafkaETLConsumer.CONF_HADOOP_CONF_DIR, "/etc/hadoop/conf");
        parquetProps.put(KafkaETLConsumer.CONF_OUTPUT, output);
        parquetProps.put(KafkaETLConsumer.CONF_DATE_FORMAT, "yyyy-MM-dd/HH/mm");
        parquetProps.put(KafkaETLConsumer.CONF_INTERVAL_UNIT, KafkaETLConsumer.IntervalUnit.MINUTE.name());
        parquetProps.put(KafkaETLConsumer.CONF_INTERVAL, "1");

        // TODO: add implementation.
        AvroDeserializeService avroDeserializeService = null;

        KafkaETLConsumer kafkaETLConsumer = new KafkaETLConsumer(props, topics, timeout, parquetProps, avroDeserializeService);
        kafkaETLConsumer.run();

        Thread.sleep(Long.MAX_VALUE);
    }

}
