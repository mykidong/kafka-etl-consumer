package kafka.etl;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.nio.ByteBuffer;

/**
 * Created by mykidong on 2016-08-01.
 */
public interface AvroDeserializeService {

    public GenericRecord deserializeAvro(String eventType, byte[] avroBytes);

    public Schema getSchema(String eventType);

}
