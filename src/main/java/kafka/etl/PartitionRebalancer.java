package kafka.etl;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * Created by mykidong on 2016-08-03.
 */
public class PartitionRebalancer implements ConsumerRebalanceListener{

    private static Logger log = LoggerFactory.getLogger(PartitionRebalancer.class);


    private ETLTask etlTask;

    private final Object lock = new Object();

    public PartitionRebalancer(ETLTask etlTask)
    {
        this.etlTask = etlTask;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> collection) {
        synchronized (lock) {
            this.etlTask.flushAndCommit(this.etlTask.getLatestTpMap(), true);
            log.info("parquet file rolled and offset commited...");

            this.etlTask.setNew();
        }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> collection) {
        setNewPartitionsAssigned(collection);
    }

    private void setNewPartitionsAssigned(Collection<TopicPartition> collection) {
        this.etlTask.setCurrentPartitions(collection);

        for(TopicPartition tp : collection)
        {
            String topic = tp.topic();
            int partition = tp.partition();

            log.info("new partition assigned: topic [{}], parition [{}]", topic, partition);
        }
    }
}
