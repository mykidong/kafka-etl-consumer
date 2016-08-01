package kafka.producer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.xml.DOMConfigurator;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.config.PropertiesFactoryBean;
import org.springframework.core.io.ClassPathResource;

import java.util.Date;
import java.util.Properties;
import java.util.UUID;

/**
 * Created by mykidong on 2016-08-01.
 */
public class GenericRecordKafkaProducer {

    public static final String ITEM_VIEW_EVENT_AVRO_SCHEMA = "/META-INF/avro/item-view-event.avsc";

    private Schema schema;

    private Producer<Integer, GenericRecord> producer;


    @Before
    public void init() throws Exception {
        java.net.URL url = this.getClass().getResource("/log4j-test.xml");
        DOMConfigurator.configure(url);


        Schema.Parser parser = new Schema.Parser();
        schema = parser.parse(new GenericRecordKafkaProducer().getClass().getResourceAsStream(ITEM_VIEW_EVENT_AVRO_SCHEMA));

        Properties kafkaProperties = this.getProperties("kafkaProducer.properties");

        String brokers = System.getProperty("brokers", "localhost:9092");
        kafkaProperties.put("bootstrap.servers", brokers);

        producer = new KafkaProducer<>(kafkaProperties);
    }

    private Properties getProperties(String propPath) throws Exception
    {
        PropertiesFactoryBean propBean = new PropertiesFactoryBean();
        propBean.setLocation(new ClassPathResource(propPath));
        propBean.afterPropertiesSet();

        return  propBean.getObject();
    }


    @Test
    public void produce()
    {
        int MAX = 10;

        String topic = "item-view-event";

        for(int i = 0; i <MAX; i++) {
            GenericRecord basePropRecord = new GenericData.Record(schema.getField("baseProperties").schema());
            basePropRecord.put("eventType", topic);
            basePropRecord.put("timestamp", new Date().getTime());
            basePropRecord.put("url", "http://any-url..." + i);
            basePropRecord.put("referer", "http://any-referer...");
            basePropRecord.put("uid", UUID.randomUUID().toString());
            basePropRecord.put("pcid", "any-pc-id");
            basePropRecord.put("serviceId", "any-service-id");
            basePropRecord.put("version", "1.0.0");
            basePropRecord.put("deviceType", "MOBILE");
            basePropRecord.put("domain", "kafka.com");
            basePropRecord.put("site", "m.kafka.com");


            GenericRecord iveRecord = new GenericData.Record(schema);
            iveRecord.put("baseProperties", basePropRecord);
            iveRecord.put("itemId", "any-item-id" + i);
            iveRecord.put("categoryId", "any-category-id");
            iveRecord.put("brandId", "any-brand-id");
            iveRecord.put("itemType", "any-item-type");
            iveRecord.put("promotionId", "any-promotion-id");
            iveRecord.put("price", 168000L + i);
            iveRecord.put("itemTitle", "any item title...");
            iveRecord.put("itemDescription", "any item desc....");
            iveRecord.put("thumbnailUrl", "http://any-thumbnail-url...");

            producer.send(new ProducerRecord<Integer, GenericRecord>(topic, iveRecord));
        }

        System.out.println("produced...");
    }
}
