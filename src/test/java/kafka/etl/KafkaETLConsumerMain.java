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
        java.net.URL url = new KafkaETLConsumerMain().getClass().getResource("/log4j-test.xml");
        System.out.println("log4j url: " + url.toString());

        DOMConfigurator.configure(url);

        KafkaETLParquetConsumer.main(args);
    }
}
