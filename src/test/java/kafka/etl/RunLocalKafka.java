package kafka.etl;


import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import org.apache.log4j.xml.DOMConfigurator;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.config.PropertiesFactoryBean;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.util.Properties;

public class RunLocalKafka {

    @Before
    public void init() throws Exception {
        java.net.URL url = new RunLocalKafka().getClass().getResource("/log4j-test.xml");
        System.out.println("log4j url: " + url.toString());

        DOMConfigurator.configure(url);
    }


    @Test
    public void startKafka() throws Exception {

        String kafkaDataPath = System.getProperty("kafkaDataPath", "/tmp/kafka-data");

        PropertiesFactoryBean kafkaPropBean = new PropertiesFactoryBean();
        kafkaPropBean.setLocation(new ClassPathResource("kafkaPropLocal.properties"));
        kafkaPropBean.afterPropertiesSet();

        Properties kafkaProperties = kafkaPropBean.getObject();

        kafkaProperties.put("log.dir", kafkaDataPath);

        try {
            //start kafka
            KafkaLocal kafka = new KafkaLocal(kafkaProperties);

            Thread.sleep(5000);
        } catch (Exception e){
            e.printStackTrace(System.out);
            System.err.println("Error running local Kafka broker");
        }
    }


    private static class KafkaLocal {

        public KafkaServerStartable kafka;


        public KafkaLocal(Properties kafkaProperties) throws IOException, InterruptedException{
            KafkaConfig kafkaConfig = new KafkaConfig(kafkaProperties);

            //start local kafka broker
            kafka = new KafkaServerStartable(kafkaConfig);
            System.out.println("starting local kafka broker...");
            kafka.startup();

            System.out.println("done");
        }


        public void stop(){
            //stop kafka broker
            System.out.println("stopping kafka...");
            kafka.shutdown();
            System.out.println("done");
        }

    }
}
