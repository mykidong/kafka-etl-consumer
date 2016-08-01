# kafka-etl-consumer

This Kafka ETL consumes avro encoded data from Kafka and saves it to Parquet on HDFS.
It acts like camus which is mapreduce library.

It is inspired from [Kafka Connect](http://docs.confluent.io/3.0.0/connect/) which almost depends on confluent's [Schema Registry](http://docs.confluent.io/1.0/schema-registry/docs/index.html). 

This Kafka ETL is independent of confluent's schema registry, and the implementations of avro schema registry are classpath and consul from which avro schema will be retrieved by topic name.

It could be scaled out with docker container in which this kafka etl runs.

It supports multiple topics with data pattern.

The output parquet file convention is like this:
```
<output-base-dir>/<topic>/<data-pattern>/part-<partition-number>-<sequence-number>.parquet
```
for instance,
```
hdfs dfs -ls -R /data/kafka-etl-parquet-test;
drwxr-xr-x   - etl hadoop          0 2016-08-01 15:11 /data/kafka-etl-parquet-test/item-view-event
drwxr-xr-x   - etl hadoop          0 2016-08-01 15:11 /data/kafka-etl-parquet-test/item-view-event/2016-08-01
drwxr-xr-x   - etl hadoop          0 2016-08-01 15:11 /data/kafka-etl-parquet-test/item-view-event/2016-08-01/15
drwxr-xr-x   - etl hadoop          0 2016-08-01 15:11 /data/kafka-etl-parquet-test/item-view-event/2016-08-01/15/11
-rw-r--r--   3 etl hadoop       5619 2016-08-01 15:12 /data/kafka-etl-parquet-test/item-view-event/2016-08-01/15/11/part-0-0.parquet
```


It is assumed that the messages produced to kafka should be encoded with avro GenericRecord(falls SpecificRecord, then SpecificDatumWriter should be used in the producer serializer), and the producer serializer should be like kafka.serializer.KafkaAvroEventSerializer.


## Run Example

### Run Kafka on Local
If Kafka instance is not available, run kafka on local.
```
mvn -e -Dtest=RunLocalKafka test;
```

### Run Kafka ETL Consumer
Kafka ETL Consumer Test case should be run on Hadoop Nodes.

Kafka Consumer Properties should be set correctly:
```
        // kafka consumer properties.
        Properties kafkaConsumerProps = new Properties();
        kafkaConsumerProps.put("bootstrap.servers", brokers);
        kafkaConsumerProps.put("group.id", "kafka-etl-test-group");
        kafkaConsumerProps.put("enable.auto.commit", "false");
        kafkaConsumerProps.put("session.timeout.ms", "30000");
        kafkaConsumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        kafkaConsumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
```
, where enable.auto.commit must be false.

To set topics whose data should be consumed from kafka:
```
        // topic list which should consume.
        List<String> topics = new ArrayList<>();
        topics.add("item-view-event");
```
, where multiple topics can be added.


To set kafka consumer poll timeout:
```
        // kafka consumer poll timeout.
        long pollTimeout = 1000;
```

To set hdfs and parquet related properties:
```
        // parquet sink related properties.
        Map<String,String> parquetProps = new HashMap<>();
        parquetProps.put(KafkaETLParquetConsumer.CONF_HADOOP_CONF_DIR, "/etc/hadoop/conf");
        parquetProps.put(KafkaETLParquetConsumer.CONF_OUTPUT, output);
        parquetProps.put(KafkaETLParquetConsumer.CONF_DATE_FORMAT, "yyyy-MM-dd/HH/mm");
        parquetProps.put(KafkaETLParquetConsumer.CONF_INTERVAL_UNIT, KafkaETLParquetConsumer.IntervalUnit.MINUTE.name());
        parquetProps.put(KafkaETLParquetConsumer.CONF_INTERVAL, "1");
```
, where hadoop conf directory KafkaETLParquetConsumer.CONF_HADOOP_CONF_DIR should be set correctly.
At every minute, parquet files will be rolled onto hdfs.

To set topic avro path properties, in this case, classpath avro schema registry is used:
```
        // topic and avro schema classpath properties.
        // topic key must be in topic list.
        Properties topicAndPathProps = new Properties();
        topicAndPathProps.put("item-view-event", "/META-INF/avro/item-view-event.avsc");
```
, where topic key must exist in topic list above and the avro schema avsc file must be in the classpath.


and finally run kafka etl consumer:
```
        KafkaETLParquetConsumer kafkaETLConsumer =
                new KafkaETLParquetConsumer(kafkaConsumerProps, topics, pollTimeout, parquetProps, new ClasspathAvroDeserializeService(topicAndPathProps));
        kafkaETLConsumer.run();
```
 
Let's see the entire test case:
```
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

        Thread.sleep(Long.MAX_VALUE);
    }
```


Run Kafka ETL Parquet Consumer Test Case.
```
mvn -e -Dtest=KafkaETLParquetConsumerTestSkip#runForClasspath -Dbrokers=localhost:9092 test;
```


### Run Kafka Avro Message Producer
Run Kafka avro message producer.
```
mvn -e -Dtest=GenericRecordKafkaProducer -Dbrokers=localhost:9092 test;
```


### Consul Avro Schema Registry Implementation
To use Consul which can be used as Avro Schema Registry, kafka etl consumer runs as follows:
```
        KafkaETLParquetConsumer kafkaETLConsumer =
                new KafkaETLParquetConsumer(kafkaConsumerProps, topics, pollTimeout, parquetProps, new ConsulAvroDeserializeService(topicAndPathProps, agentHost, agentPort));
        kafkaETLConsumer.run();
```

Run Test Case.
```
mvn -e -Dtest=KafkaETLParquetConsumerTestSkip#runForConsul -Dbrokers=localhost:9092 test;
```


