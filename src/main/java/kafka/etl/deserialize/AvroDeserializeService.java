package kafka.etl.deserialize;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.nio.ByteBuffer;

/**
 * Created by mykidong on 2016-08-01.
 */
public interface AvroDeserializeService {

    /**
     * deserialize avro bytes to generic record.
     *
     * @param topic topic name.
     * @param avroBytes avro encoded bytes.
     * @return
     */
    public GenericRecord deserializeAvro(String topic, byte[] avroBytes);

    /**
     * get avro schema by topic name.
     *
     * @param topic topic name.
     * @return
     */
    public Schema getSchema(String topic);

}
