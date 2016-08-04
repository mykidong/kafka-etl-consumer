package kafka.etl;

import kafka.etl.deserialize.AvroDeserializeService;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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

/**
 * Created by mykidong on 2016-08-03.
 */
public class ETLTask {

    private static Logger log = LoggerFactory.getLogger(ETLTask.class);

    private Properties kafkaConsumerProps;
    private List<String> topics;
    private long timeout;

    private AvroDeserializeService avroDeserializeService;

    private Map<String,String> parquetProps;

    private CompressionCodecName compressionCodecName;
    private int blockSize;
    private int pageSize;
    private Configuration conf;

    private Map<TopicPartition, ETLTask.ParquetWriterInfo> writerMap;

    private AtomicLong seq = new AtomicLong(0);

    private long rollingIntervalInMillis;

    private long currentTime;

    private boolean shouldCreateWriters = true;

    private Map<TopicPartition, OffsetAndMetadata> latestTpMap;

    private Collection<TopicPartition> currentPartitions;

    private final Object lock = new Object();

    private KafkaConsumer<Integer, byte[]> consumer;

    public ETLTask(Properties kafkaConsumerProps, List<String> topics, long timeout, AvroDeserializeService avroDeserializeService, Map<String,String> parquetProps)
    {
        this.kafkaConsumerProps = kafkaConsumerProps;
        this.topics = topics;
        this.timeout = timeout;
        this.avroDeserializeService = avroDeserializeService;
        this.parquetProps = parquetProps;

        this.consumer = new KafkaConsumer<>(this.kafkaConsumerProps);

        calcRollingIntervalInMillis();
    }


    public void setCurrentPartitions(Collection<TopicPartition> topicPartitions)
    {
        this.currentPartitions = topicPartitions;
    }

    public KafkaConsumer<Integer, byte[]> getConsumer()
    {
        return this.consumer;
    }


    public Map<TopicPartition, OffsetAndMetadata> getLatestTpMap()
    {
        if(this.latestTpMap == null)
        {
            this.latestTpMap = new HashMap<>();
        }

        return this.latestTpMap;
    }

    private static class ParquetWriterInfo
    {
        private ParquetWriter<GenericRecord> writer;
        private String path;

        public ParquetWriter<GenericRecord> getWriter() {
            return writer;
        }

        public void setWriter(ParquetWriter<GenericRecord> writer) {
            this.writer = writer;
        }

        public String getPath() {
            return path;
        }

        public void setPath(String path) {
            this.path = path;
        }
    }


    private void calcRollingIntervalInMillis() {
        String intervalUnit = this.parquetProps.get(KafkaETLParquetConsumer.CONF_INTERVAL_UNIT);
        int interval = Integer.parseInt((this.parquetProps.get(KafkaETLParquetConsumer.CONF_INTERVAL)));

        if(intervalUnit.equals(KafkaETLParquetConsumer.IntervalUnit.DAY.name()))
        {
            this.rollingIntervalInMillis = 24 * 60 * 60 * 1000 * interval;
        }
        else if(intervalUnit.equals(KafkaETLParquetConsumer.IntervalUnit.HOUR.name()))
        {
            this.rollingIntervalInMillis = 60 * 60 * 1000 * interval;
        }
        else if(intervalUnit.equals(KafkaETLParquetConsumer.IntervalUnit.MINUTE.name()))
        {
            this.rollingIntervalInMillis = 60 * 1000 * interval;
        }
    }

    private void openParquetWriters(AvroDeserializeService avroDeserializeService) {
        // compression codec.
        compressionCodecName = CompressionCodecName.SNAPPY;
        blockSize = (this.parquetProps.get(KafkaETLParquetConsumer.CONF_BLOCK_SIZE) != null) ? Integer.parseInt(this.parquetProps.get(KafkaETLParquetConsumer.CONF_BLOCK_SIZE)) : 256 * 1024 * 1024;
        pageSize = (this.parquetProps.get(KafkaETLParquetConsumer.CONF_PAGE_SIZE) != null) ? Integer.parseInt(this.parquetProps.get(KafkaETLParquetConsumer.CONF_PAGE_SIZE)) : 64 * 1024;

        String hadoopConfDir = this.parquetProps.get(KafkaETLParquetConsumer.CONF_HADOOP_CONF_DIR);

        // hadoop configuration.
        this.conf = new Configuration();
        this.conf.addResource(new Path(hadoopConfDir + "/core-site.xml"));
        this.conf.addResource(new Path(hadoopConfDir + "/hdfs-site.xml"));


        String output = this.parquetProps.get(KafkaETLParquetConsumer.CONF_OUTPUT);

        String dateFormat = this.parquetProps.get(KafkaETLParquetConsumer.CONF_DATE_FORMAT);

        SimpleDateFormat df = new SimpleDateFormat(dateFormat);

        Date currentDate = new Date();
        this.currentTime = currentDate.getTime();

        String dateString = df.format(currentDate);

        writerMap = new HashMap<>();

        for(TopicPartition tp : this.currentPartitions) {
            String topic = tp.topic();
            int partition = tp.partition();

            String path = null;

            while(true)
            {
                path = getPath(output, dateString, topic, partition);
                if(!this.fileExists(path))
                {
                    break;
                }
                else
                {
                    log.info("file [{}] already exists!", path);
                }
            }


            // avro schema.
            Schema avroSchema = avroDeserializeService.getSchema(topic);

            try {
                ParquetWriter<GenericRecord> writer = new AvroParquetWriter<>(new Path(path), avroSchema, compressionCodecName, blockSize, pageSize, true, this.conf);

                ParquetWriterInfo parquetWriterInfo = new ParquetWriterInfo();
                parquetWriterInfo.setWriter(writer);
                parquetWriterInfo.setPath(path);

                writerMap.put(tp, parquetWriterInfo);

                log.info("created writer: [{}]", path);
            } catch (Exception e) {
                log.error("error: " + e.getMessage());
                throw new RuntimeException(e);
            }
        }
    }

    private String getPath(String output, String dateString, String topic, int partition) {
        long currentSeq = seq.getAndIncrement();

        String parquetPartFileName = "part-" + partition + "-" + currentSeq + ".parquet";

        return output + "/" + topic + "/" + dateString + "/" + parquetPartFileName;
    }

    private boolean fileExists(String path)
    {
        try {
            FileSystem fs = FileSystem.get(this.conf);

            return fs.exists(new Path(path));
        }catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

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


                    ParquetWriter<GenericRecord> writer = this.writerMap.get(tp).getWriter();
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
            log.info("wake up exception occurred: [{}]", e.getMessage());
        } finally {
            try {
                flushAndCommit(latestTpMap, true);
                log.info("parquet file rolled and offset committed before consumer closes...");
            } finally {
                consumer.close();

                log.info("Closed consumer and we are done");
            }
        }
    }

    public void setNew()
    {
        this.shouldCreateWriters = true;
        this.writerMap = null;
    }


    public void flushAndCommit(Map<TopicPartition, OffsetAndMetadata> latestTpMap, boolean commitSync) {
        if (writerMap != null && latestTpMap.size() > 0) {
            for (TopicPartition tp : writerMap.keySet()) {

                ParquetWriterInfo parquetWriterInfo = writerMap.get(tp);

                ParquetWriter<GenericRecord> writer = parquetWriterInfo.getWriter();

                String meta = tp.toString() + ":" + new Date().toString();

                String path = parquetWriterInfo.getPath();

                try {
                    FileSystem fs = FileSystem.get(this.conf);

                    if(!fs.exists(new Path(path)))
                    {
                        continue;
                    }

                    writer.close();

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

            writerMap = null;
        }
    }
}
