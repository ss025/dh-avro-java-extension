package in.dailyhunt.avro;

import in.dailyhunt.avro.Schema.Parser;
import in.dailyhunt.avro.data.LruSet;
import in.dailyhunt.avro.data.LruValue;
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

public class LruSetTestSchema {


  @Test
  public void testLruSetSchemaSupport() {
    String json = "{\"name\":\"item\",\"type\":\"lru_set\",\"limit\":\"30days\"}";
    final Parser parser = new Parser();
    final Schema schema = parser.parse(json);
    Assert.assertNotNull(schema);
  }

  @Test
  public void testWriteToLruSet() throws IOException {
    String json = "{\"type\":\"record\",\"namespace\":\"Test\",\"name\":\"Test\",\"fields\":["
        + "{\"name\":\"name\",\"type\":\"string\"},"
        + "{\"name\":\"ids\",\"type\":\"lru_set\"}]}";

    final Parser parser = new Parser();
    final Schema schema = parser.parse(json);

    final LruSet ids = new LruSet();
    ids.addWithDefault("111");
    ids.addWithDefault("222");

    GenericRecord record = new Record(schema);
    record.put("name", "test");
    record.put("ids", ids);

    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
    Encoder encoder = EncoderFactory.get().binaryEncoder(outStream, null);
    writer.write(record, encoder);
    encoder.flush();
    outStream.close();
  }


  @SuppressWarnings("unchecked")
  @Test
  public void testWriteAndReadToSet() throws IOException {
    String json = "{\"type\":\"record\",\"namespace\":\"Test\",\"name\":\"Test\",\"fields\":["
        + "{\"name\":\"name\",\"type\":\"string\"},"
        + "{\"name\":\"ids\",\"type\":\"lru_set\"}]}";

    final Parser parser = new Parser();
    final Schema schema = parser.parse(json);

    final LruSet ids = new LruSet();
    ids.put("111", new LruValue(12344455L, 1L));
    ids.addWithDefault("222");

    GenericRecord record = new Record(schema);
    record.put("name", "test");
    record.put("ids", ids);

    // Write
    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
    Encoder encoder = EncoderFactory.get().binaryEncoder(outStream, null);
    writer.write(record, encoder);
    encoder.flush();
    outStream.close();

    // Read
    DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
    final Decoder binaryDecoder = DecoderFactory.get().binaryDecoder(outStream.toByteArray(), null);
    final GenericRecord readRecord = reader.read(null, binaryDecoder);

    Assert.assertEquals("test", readRecord.get("name").toString());

    final LruSet lruSet = (LruSet) readRecord.get("ids");
    Assert.assertFalse("Set which is read from avro should not be empty", lruSet.isEmpty());

    final LruValue lruValue111 = lruSet.get("111");
    Assert.assertEquals(12344455L, lruValue111.getLastAccess());
    Assert.assertEquals(1, lruValue111.getCount());

    final LruValue lruValue222 = lruSet.get("222");
    Assert.assertTrue(lruValue222.getLastAccess() < System.currentTimeMillis());
    Assert.assertEquals(1, lruValue222.getCount());

  }


}
