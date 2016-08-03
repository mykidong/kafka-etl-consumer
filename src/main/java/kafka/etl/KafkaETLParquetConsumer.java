package kafka.etl;


import kafka.etl.deserialize.AvroDeserializeService;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class KafkaETLParquetConsumer {

    private static Logger log = LoggerFactory.getLogger(KafkaETLParquetConsumer.class);

    public static final String CONF_HADOOP_CONF_DIR = "hadoop.conf.dir";
    public static final String CONF_BLOCK_SIZE = "block.size";
    public static final String CONF_PAGE_SIZE = "page.size";
    public static final String CONF_OUTPUT = "output";
    public static final String CONF_DATE_FORMAT = "date.format";
    public static final String CONF_INTERVAL_UNIT = "interval.unit";
    public static final String CONF_INTERVAL = "interval";

    private Thread consumerThread;

    private Properties kafkaConsumerProps;
    private List<String> topics;
    private long pollTimeout;
    private Map<String,String> parquetProps;
    private AvroDeserializeService avroDeserializeService;

    private KafkaConsumer<Integer, byte[]> consumer;


    public static enum IntervalUnit {
        DAY("DAY"), HOUR("HOUR"), MINUTE("MINUTE");

        String unit;

        private IntervalUnit(String unit)
        {
            this.unit = unit;
        }
    }

    public KafkaETLParquetConsumer(Properties kafkaConsumerProps, List<String> topics, long pollTimeout, Map<String,String> parquetProps, AvroDeserializeService avroDeserializeService)
    {
        this.kafkaConsumerProps = kafkaConsumerProps;

        // set auto commit to false.
        this.kafkaConsumerProps.put("enable.auto.commit", "false");

        consumer = new KafkaConsumer<>(this.kafkaConsumerProps);


        this.topics = topics;
        this.pollTimeout = pollTimeout;
        this.parquetProps = parquetProps;

        this.avroDeserializeService = avroDeserializeService;
    }

    public void run()
    {
        consumerThread = new Thread(new ETLTask(this.consumer, topics, pollTimeout, avroDeserializeService, parquetProps));
        consumerThread.start();

        final Thread mainThread = Thread.currentThread();

        // Registering a shutdown hook so we can exit cleanly
        Runtime.getRuntime().addShutdownHook(new ShutdownHookThread(this, mainThread));

        log.info("kafka etl consumer started...");
    }


    private static class ShutdownHookThread extends Thread
    {
        private KafkaETLParquetConsumer kafkaETLParquetConsumer;
        private Thread mainThread;

        public ShutdownHookThread(KafkaETLParquetConsumer kafkaETLParquetConsumer, Thread mainThread)
        {
            this.kafkaETLParquetConsumer = kafkaETLParquetConsumer;
            this.mainThread = mainThread;
        }

        public void run() {
            log.info("Starting exit...");
            // Note that shutdownhook runs in a separate thread, so the only thing we can safely do to a consumer is wake it up
            kafkaETLParquetConsumer.consumer.wakeup();
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


    public void stop()
    {
        if(consumerThread != null) {
            consumerThread = null;
        }
    }

    private static class ETLTask implements Runnable
    {
        private List<String> topics;
        private long timeout;

        private KafkaConsumer<Integer, byte[]> consumer;

        private AvroDeserializeService avroDeserializeService;

        private Map<String,String> parquetProps;

        private CompressionCodecName compressionCodecName;
        private int blockSize;
        private int pageSize;
        private Configuration conf;

        private Map<TopicPartition, ParquetWriter<GenericRecord>> writerMap;

        private AtomicLong seq = new AtomicLong(0);

        private long rollingIntervalInMillis;

        private long currentTime;

        private boolean shouldCreateWriters = true;

        private Map<TopicPartition, OffsetAndMetadata> latestTpMap = new HashMap<>();

        private Collection<TopicPartition> currentPartitions;


        public void setCurrentPartitions(Collection<TopicPartition> topicPartitions)
        {
            this.currentPartitions = topicPartitions;
        }


        public ETLTask(KafkaConsumer<Integer, byte[]> consumer, List<String> topics, long timeout, AvroDeserializeService avroDeserializeService, Map<String,String> parquetProps)
        {
            this.topics = topics;
            this.timeout = timeout;
            this.avroDeserializeService = avroDeserializeService;
            this.parquetProps = parquetProps;

            this.consumer = consumer;

            calcRollingIntervalInMillis();
        }

        public Map<TopicPartition, OffsetAndMetadata> getLatestTpMap()
        {
            return this.latestTpMap;
        }


        private void calcRollingIntervalInMillis() {
            String intervalUnit = this.parquetProps.get(CONF_INTERVAL_UNIT);
            int interval = Integer.parseInt((this.parquetProps.get(CONF_INTERVAL)));

            if(intervalUnit.equals(IntervalUnit.DAY.name()))
            {
                this.rollingIntervalInMillis = 24 * 60 * 60 * 1000 * interval;
            }
            else if(intervalUnit.equals(IntervalUnit.HOUR.name()))
            {
                this.rollingIntervalInMillis = 60 * 60 * 1000 * interval;
            }
            else if(intervalUnit.equals(IntervalUnit.MINUTE.name()))
            {
                this.rollingIntervalInMillis = 60 * 1000 * interval;
            }
        }

        private void openParquetWriters(AvroDeserializeService avroDeserializeService) {
            // compression codec.
            compressionCodecName = CompressionCodecName.SNAPPY;
            blockSize = (this.parquetProps.get(CONF_BLOCK_SIZE) != null) ? Integer.parseInt(this.parquetProps.get(CONF_BLOCK_SIZE)) : 256 * 1024 * 1024;
            pageSize = (this.parquetProps.get(CONF_PAGE_SIZE) != null) ? Integer.parseInt(this.parquetProps.get(CONF_PAGE_SIZE)) : 64 * 1024;

            String hadoopConfDir = this.parquetProps.get(CONF_HADOOP_CONF_DIR);

            // hadoop configuration.
            this.conf = new Configuration();
            this.conf.addResource(new Path(hadoopConfDir + "/core-site.xml"));
            this.conf.addResource(new Path(hadoopConfDir + "/hdfs-site.xml"));


            String output = this.parquetProps.get(CONF_OUTPUT);

            String dateFormat = this.parquetProps.get(CONF_DATE_FORMAT);

            SimpleDateFormat df = new SimpleDateFormat(dateFormat);

            Date currentDate = new Date();
            this.currentTime = currentDate.getTime();

            String dateString = df.format(currentDate);

            writerMap = new HashMap<>();

            for(TopicPartition tp : this.currentPartitions) {
                String topic = tp.topic();
                int partition = tp.partition();

                long currentSeq = seq.getAndIncrement();

                String parquetPartFileName = "part-" + partition + "-" + currentSeq + ".parquet";

                String path = output + "/" + topic + "/" + dateString + "/" + parquetPartFileName;

                // avro schema.
                Schema avroSchema = avroDeserializeService.getSchema(topic);

                try {
                    ParquetWriter<GenericRecord> writer = new AvroParquetWriter<>(new Path(path), avroSchema, compressionCodecName, blockSize, pageSize, true, this.conf);

                    writerMap.put(tp, writer);

                    log.info("created writer: [{}]", tp.toString());
                } catch (Exception e) {
                    log.error("error: " + e.getMessage());
                    throw new RuntimeException(e);
                }
            }
        }

        @Override
        public void run() {

            try {
                consumer.subscribe(this.topics, new PartitionRebalancer(this));

                while (true) {
                    ConsumerRecords<Integer, byte[]> records = consumer.poll(this.timeout);

                    if (records.count() > 0) {
                        if (this.shouldCreateWriters) {
                            log.info("new writers opened...");
                            this.openParquetWriters(this.avroDeserializeService);

                            this.shouldCreateWriters = false;
                        }
                    }


                    for (ConsumerRecord<Integer, byte[]> record : records) {
                        String topic = record.topic();
                        int partition = record.partition();
                        byte[] value = record.value();
                        long offset = record.offset();

                        TopicPartition tp = new TopicPartition(topic, partition);

                        latestTpMap.put(tp, new OffsetAndMetadata(offset));

                        GenericRecord genericRecord = avroDeserializeService.deserializeAvro(topic, value);


                        ParquetWriter<GenericRecord> writer = this.writerMap.get(tp);
                        try {
                            //log.info("value in json: [{}]", genericRecord.toString());

                            writer.write(genericRecord);
                        } catch (Exception e) {
                            log.error("error: " + e.getMessage());
                            throw new RuntimeException(e);
                        }
                    }

                    long timestamp = new Date().getTime();
                    long diff = timestamp - this.currentTime;
                    if (diff > this.rollingIntervalInMillis) {
                        if (!this.shouldCreateWriters) {
                            flushAndCommit(latestTpMap, false);

                            this.currentTime = timestamp;
                            this.shouldCreateWriters = true;
                        }
                    }
                }
            } catch (WakeupException e) {
                // ignore for shutdown
            } finally {
                try {
                    flushAndCommit(latestTpMap, true);
                } finally {
                    consumer.close();
                }
                log.info("Closed consumer and we are done");
            }
        }

        private static class PartitionRebalancer implements ConsumerRebalanceListener
        {
            private ETLTask etlTask;

            public PartitionRebalancer(ETLTask etlTask)
            {
                this.etlTask = etlTask;
            }

            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                this.etlTask.flushAndCommit(this.etlTask.getLatestTpMap(), true);
                log.info("parquet file rolled and offset commited...");
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                this.etlTask.setCurrentPartitions(collection);

                for(TopicPartition tp : collection)
                {
                    String topic = tp.topic();
                    int partition = tp.partition();

                    log.info("new partition assigned: topic [{}], parition [{}]", topic, partition);
                }
            }
        }



        public void flushAndCommit(Map<TopicPartition, OffsetAndMetadata> latestTpMap, boolean commitSync) {
            if(writerMap != null) {
                for (TopicPartition tp : writerMap.keySet()) {
                    ParquetWriter<GenericRecord> writer = writerMap.get(tp);

                    String meta = tp.toString() + ":" + new Date().toString();

                    if(writer != null) {
                        try {
                            try {
                                writer.close();
                            }catch (NullPointerException ne)
                            {
                                return;
                            }

                            log.info("closed writer: [{}]", tp.toString());

                            OffsetAndMetadata offset = latestTpMap.get(tp);

                            Map<TopicPartition, OffsetAndMetadata> commitTp = new HashMap<>();
                            commitTp.put(tp, new OffsetAndMetadata(offset.offset(), meta));

                            if (commitSync) {
                                consumer.commitSync(commitTp);
                            } else {

                                consumer.commitAsync(commitTp, new OffsetCommitCallback() {
                                    @Override
                                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
                                        for (TopicPartition commitTp : map.keySet()) {
                                            log.info("commited topic and partition: [{}], offset: [{}]", commitTp.toString(), map.get(commitTp).toString());
                                        }
                                    }
                                });
                            }
                        } catch (Exception e) {
                            log.error("error: " + e);
                            throw new RuntimeException(e);
                        }
                    }
                }
            }
        }
    }
}
