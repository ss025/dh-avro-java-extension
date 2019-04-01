package in.dailyhunt.avro;

import in.dailyhunt.avro.Schema.Parser;
import in.dailyhunt.avro.generic.GenericData.Record;
import in.dailyhunt.avro.generic.GenericDatumReader;
import in.dailyhunt.avro.generic.GenericDatumWriter;
import in.dailyhunt.avro.generic.GenericRecord;
import in.dailyhunt.avro.io.DatumReader;
import in.dailyhunt.avro.io.DatumWriter;
import in.dailyhunt.avro.io.Decoder;
import in.dailyhunt.avro.io.DecoderFactory;
import in.dailyhunt.avro.io.Encoder;
import in.dailyhunt.avro.io.EncoderFactory;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;

public class DateTimeTest {

  @Test
  public void testAvroLogicalType() throws IOException {
    String schemaJson = "{\"type\":\"record\",\"namespace\":\"Test\",\"name\":\"Test\",\"fields\":[{\"name\":\"a\",\"type\":\"date\"}]}";
    final Schema writerSchema = new Parser().parse(schemaJson);
    System.out.println(writerSchema);

    final long currentTimeMillis = System.currentTimeMillis();
    System.out.println("Saving   "+currentTimeMillis);

    final Record record = new Record(writerSchema);
    record.put("a", currentTimeMillis);

    // Write
    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(writerSchema);
    Encoder encoder = EncoderFactory.get().binaryEncoder(outStream, null);
    writer.write(record, encoder);
    encoder.flush();
    outStream.close();

    // Read
    // Read
    DatumReader<GenericRecord> reader = new GenericDatumReader<>(writerSchema);
    final Decoder binaryDecoder = DecoderFactory.get().binaryDecoder(outStream.toByteArray(), null);
    final GenericRecord readRecord = reader.read(null, binaryDecoder);

    final long epochInMillis = (long) readRecord.get("a");
    Assert.assertEquals(currentTimeMillis,epochInMillis);
    System.out.println("Received "+epochInMillis);

  }

}
