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
import in.dailyhunt.avro.util.Utf8;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

public class SetDataTypeSchemaTest {


  @Test
  public void testSetTypeSupport() {
    String json = "{\"type\": \"set\", \"items\": \"int\" }";
    final Parser parser = new Parser();
    final Schema schema = parser.parse(json);
    Assert.assertNotNull(schema);
  }


  @Test
  public void testWriteToSet() throws IOException {
    String json = "{\"type\":\"record\",\"namespace\":\"Test\",\"name\":\"Test\",\"fields\":["
        + "{\"name\":\"name\",\"type\":\"string\"},"
        + "{\"name\":\"ids\",\"type\":\"set\"}]}";

    final Parser parser = new Parser();
    final Schema schema = parser.parse(json);

    final Set<String> ids = new HashSet<>();
    ids.add("111");
    ids.add("222");

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
        + "{\"name\":\"ids\",\"type\":\"set\"}]}";

    final Parser parser = new Parser();
    final Schema schema = parser.parse(json);

    final Set<String> ids = new HashSet<>();
    ids.add("111");
    ids.add("222");

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
    final Set<Utf8> set = (Set) readRecord.get("ids");

    Assert.assertFalse("Set which is read from avro should not be empty", set.isEmpty());

    for (Utf8 s : set) {
      Assert.assertTrue("value " + s + " is not present in write set", ids.contains(s.toString()));
    }


  }


  @SuppressWarnings("unchecked")
  @Test
  public void testSetToArraySchemaResolution() throws IOException {
    String json = "{\"type\":\"record\",\"namespace\":\"Test\",\"name\":\"Test\",\"fields\":["
        + "{\"name\":\"name\",\"type\":\"string\"},"
        + "{\"name\":\"ids\",\"type\":\"set\"}]}";

    final Parser parser = new Parser();
    final Schema writerSchema = parser.parse(json);

    final Set<String> ids = new HashSet<>();
    ids.add("111");
    ids.add("222");

    GenericRecord record = new Record(writerSchema);
    record.put("name", "test");
    record.put("ids", ids);

    // Write
    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(writerSchema);
    Encoder encoder = EncoderFactory.get().binaryEncoder(outStream, null);
    writer.write(record, encoder);
    encoder.flush();
    outStream.close();


    String readJson = "{\"type\":\"record\",\"namespace\":\"Test\",\"name\":\"Test\",\"fields\":["
        + "{\"name\":\"name\",\"type\":\"string\"},"
        + "{\"name\":\"ids\",\"type\":{\"type\":\"array\",\"items\":\"string\"}}]}";

    final Schema readerSchema = new Parser().parse(readJson);

    // Read
    DatumReader<GenericRecord> reader = new GenericDatumReader<>(readerSchema);
    final Decoder binaryDecoder = DecoderFactory.get().binaryDecoder(outStream.toByteArray(), null);
    final GenericRecord readRecord = reader.read(null, binaryDecoder);

    Assert.assertEquals("test", readRecord.get("name").toString());
    final List<Utf8> list = (List<Utf8>) readRecord.get("ids");

    for(Utf8 i : list){
      Assert.assertTrue(ids.contains(i.toString()));
    }



  }

  @SuppressWarnings("unchecked")
  @Test
  public void testArrayToSchemaResolution() throws IOException {
    String json = "{\"type\":\"record\",\"namespace\":\"Test\",\"name\":\"Test\",\"fields\":["
        + "{\"name\":\"name\",\"type\":\"string\"},"
        + "{\"name\":\"ids\",\"type\":{\"type\":\"array\",\"items\":\"string\"}}]}";

    final Parser parser = new Parser();
    final Schema writerSchema = parser.parse(json);

    final List<String> ids = new ArrayList<>();
    ids.add("111");
    ids.add("222");

    GenericRecord record = new Record(writerSchema);
    record.put("name", "test");
    record.put("ids", ids);

    // Write
    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(writerSchema);
    Encoder encoder = EncoderFactory.get().binaryEncoder(outStream, null);
    writer.write(record, encoder);
    encoder.flush();
    outStream.close();


    String readJson = "{\"type\":\"record\",\"namespace\":\"Test\",\"name\":\"Test\",\"fields\":["
        + "{\"name\":\"name\",\"type\":\"string\"},"
        + "{\"name\":\"ids\",\"type\":\"set\"}]}";

    final Schema readerSchema = new Parser().parse(readJson);

    // Read
    DatumReader<GenericRecord> reader = new GenericDatumReader<>(readerSchema);
    final Decoder binaryDecoder = DecoderFactory.get().binaryDecoder(outStream.toByteArray(), null);
    final GenericRecord readRecord = reader.read(null, binaryDecoder);

    Assert.assertEquals("test", readRecord.get("name").toString());
    final Set<Utf8> set = (Set<Utf8>) readRecord.get("ids");

    for(Utf8 i : set){
      Assert.assertTrue(ids.contains(i.toString()));
    }



  }


}
