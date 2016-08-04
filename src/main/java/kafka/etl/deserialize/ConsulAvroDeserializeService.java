package kafka.etl.deserialize;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.kv.model.GetValue;
import org.apache.avro.Schema;
import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * This supports consul avro schema registry from which avro schema in json will be read, and the avro schema instance will be put to map.
 *
 */
public class ConsulAvroDeserializeService extends AbstractAvroDeserializeService {

    private static Logger log = LoggerFactory.getLogger(ClasspathAvroDeserializeService.class);

    private ConsulClient client;

    public ConsulAvroDeserializeService(Properties topicAndPathProps, String agentHost, int agentPort)
    {
        super(topicAndPathProps);

        client = new ConsulClient(agentHost, agentPort);
    }


    @Override
    public void init() throws Exception {
        for(String topic : this.topicAndPathProps.stringPropertyNames())
        {

            String keyPath = this.topicAndPathProps.getProperty(topic);

            List<Map<String, Object>> values = this.getKVValues(keyPath);
            if(values == null)
            {
                throw new RuntimeException("any schema does not exist in key path [" + keyPath + "]!");
            }

            String avroSchemaString = (String) values.get(0).get("value");


            Schema.Parser parser = new Schema.Parser();
            try {
                Schema schema = parser.parse(avroSchemaString);

                schemaMap.put(topic, schema);

                log.info("loaded avro schema " + topic);

            }catch (Exception e)
            {
                log.error("failed avro schema " + topic);
                throw new RuntimeException(e);
            }
        }
    }

    private List<Map<String, Object>> getKVValues(String keyPath) {

        List<Map<String, Object>> list = new ArrayList<>();

        Response<List<GetValue>> valueResponse = client.getKVValues(keyPath);

        log.debug("consul getKVValues " + valueResponse);

        List<GetValue> getValues = valueResponse.getValue();

        if(getValues == null)
        {
            return null;
        }

        for(GetValue v : getValues)
        {
            if(v == null || v.getValue() == null)
            {
                continue;
            }

            String key = v.getKey();

            String value = new String(Base64.decodeBase64(v.getValue()));

            Map<String, Object> map = new HashMap<>();
            map.put("key", key);
            map.put("value", value);
            list.add(map);
        }

        return list;
    }
}