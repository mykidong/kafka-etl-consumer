package kafka.etl;


import kafka.etl.deserialize.AvroDeserializeService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;

public class KafkaETLParquetConsumer {

    private static Logger log = LoggerFactory.getLogger(KafkaETLParquetConsumer.class);

    public static final String CONF_HADOOP_CONF_DIR = "hadoop.conf.dir";
    public static final String CONF_BLOCK_SIZE = "block.size";
    public static final String CONF_PAGE_SIZE = "page.size";
    public static final String CONF_OUTPUT = "output";
    public static final String CONF_DATE_FORMAT = "date.format";
    public static final String CONF_INTERVAL_UNIT = "interval.unit";
    public static final String CONF_INTERVAL = "interval";

    private Properties kafkaConsumerProps;
    private List<String> topics;
    private long pollTimeout;
    private Map<String,String> parquetProps;
    private AvroDeserializeService avroDeserializeService;

    private ETLTask etlTask;


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

        this.topics = topics;
        this.pollTimeout = pollTimeout;
        this.parquetProps = parquetProps;

        this.avroDeserializeService = avroDeserializeService;
    }

    public void run()
    {
        etlTask = new ETLTask(this.kafkaConsumerProps, topics, pollTimeout, avroDeserializeService, parquetProps);
        etlTask.run();

        final Thread mainThread = Thread.currentThread();

        // Registering a shutdown hook so we can exit cleanly
        Runtime.getRuntime().addShutdownHook(new ShutdownHookThread(etlTask, mainThread));

        log.info("kafka etl consumer started...");
    }

    public void stop()
    {
        etlTask.getConsumer().wakeup();
        etlTask.setWakeupCalled(true);
    }


    private static class ShutdownHookThread extends Thread
    {
        private ETLTask etlTask;
        private Thread mainThread;

        public ShutdownHookThread(ETLTask etlTask, Thread mainThread)
        {
            this.etlTask = etlTask;
            this.mainThread = mainThread;
        }

        public void run() {
            log.info("Starting exit...");

            // wake up consumer before exit.
            etlTask.getConsumer().wakeup();

            // if consumer wakeup invokation does not throw WakeupException,
            // wakeupCalled flag set to true, in order to throw WakeupException.
            etlTask.setWakeupCalled(true);
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}
