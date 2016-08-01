package kafka.etl;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;

import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by mykidong on 2016-08-01.
 */
public abstract class AbstractAvroDeserializeService implements AvroDeserializeService {

    protected ConcurrentMap<String, Schema> schemaMap = new ConcurrentHashMap<>();

    protected Properties topicAndPathProps;


    public AbstractAvroDeserializeService(Properties topicAndPathProps)
    {
        this.topicAndPathProps = topicAndPathProps;

        try {
            this.init();
        }catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }


    abstract void init() throws Exception;

    @Override
    public Schema getSchema(String topic) {
        return schemaMap.get(topic);
    }


    @Override
    public GenericRecord deserializeAvro(String topic, byte[] avroBytes) {
        Schema schema = this.schemaMap.get(topic);

        DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
        Decoder decoder = DecoderFactory.get().binaryDecoder(avroBytes, null);

        try {
            GenericRecord genericRecord = reader.read(null, decoder);

            return genericRecord;
        }catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

}
