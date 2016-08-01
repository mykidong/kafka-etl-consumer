package kafka.serializer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.util.Map;


/**
 * Created by mykidong on 2016-05-17.
 */
public class KafkaAvroEventSerializer implements Serializer<GenericRecord> {

    private static Logger log = LoggerFactory.getLogger(KafkaAvroEventSerializer.class);


    @Override
    public void configure(Map<String, ?> map, boolean b) {
    }

    @Override
    public byte[] serialize(String s, GenericRecord genericRecord) {
        try {
            Schema schema = genericRecord.getSchema();

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
            Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            writer.write(genericRecord, encoder);
            encoder.flush();

            byte[] avroBytes = out.toByteArray();
            out.close();

            return avroBytes;
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {

    }
}
