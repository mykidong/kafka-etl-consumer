package kafka.etl.deserialize;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * This supports classpath avro schema registry from which avro schema file will be read, and the avro schema instance will be put to map.
 */
public class ClasspathAvroDeserializeService extends AbstractAvroDeserializeService {

    private static Logger log = LoggerFactory.getLogger(ClasspathAvroDeserializeService.class);

    public ClasspathAvroDeserializeService(Properties topicAndPathProps)
    {
        super(topicAndPathProps);
    }


    @Override
    public void init() throws Exception {
        for(String topic : this.topicAndPathProps.stringPropertyNames())
        {
            String schemaPath = this.topicAndPathProps.getProperty(topic);

            Schema.Parser parser = new Schema.Parser();
            try {
                Schema schema = parser.parse(getClass().getResourceAsStream(schemaPath));

                schemaMap.put(topic, schema);

                log.info("loaded avro schema " + topic);

            }catch (Exception e)
            {
                log.error("failed avro schema " + topic);
                throw new RuntimeException(e);
            }
        }
    }
}
