package in.dailyhunt.avro;

import in.dailyhunt.avro.Schema.Parser;
import in.dailyhunt.avro.Schema.Type;
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
import java.util.Optional;
import org.junit.Assert;
import org.junit.Test;

public class OptionalTest {

  @Test
  public void testOptionalSchema() {

    String schema = "{\"type\":\"record\",\"namespace\":\"Test\",\"name\":\"Test1\",\"fields\":["
        + "{\"name\":\"Name\",\"type\":\"string\"},"
        + "{\"name\":\"OpName\",\"type\":\"optional\",\"value\":\"string\"},"
        + "{\"name\":\"ids\",\"type\":\"lru_set\"}]}";

    final Schema parse = new Parser().parse(schema);
    Assert.assertNotNull(parse);
    Assert.assertTrue(parse.getClass().getSimpleName().equalsIgnoreCase("RecordSchema"));


  }

  @Test
  public void testOptionalTopLevelSchema() {

    String schema = "{\"name\":\"OpName\",\"type\":\"optional\",\"value\":\"string\"}";
    final Schema parse = new Parser().parse(schema);
    Assert.assertNotNull(parse);
    Assert.assertTrue(parse.getClass().getSimpleName().equalsIgnoreCase("optionalSchema"));

  }

  @Test
  public void testOptionalWriteSchema() throws IOException {

    String schemaStr = "{\"type\":\"record\",\"namespace\":\"Test\",\"name\":\"Test1\",\"fields\":["
        + "{\"name\":\"Name\",\"type\":\"string\"},"
        + "{\"name\":\"OpName\",\"type\":\"optional\",\"value\":\"string\"}]}";

    final Schema writerSchema = new Parser().parse(schemaStr);
    GenericRecord record = new Record(writerSchema);
    record.put("Name", "test");
    record.put("OpName", Optional.of("sohan"));

    // Write
    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(writerSchema);
    Encoder encoder = EncoderFactory.get().binaryEncoder(outStream, null);
    writer.write(record, encoder);
    encoder.flush();
    outStream.close();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testWriteAndReadWithOptional() throws IOException {
    String json = "{\"type\":\"record\",\"namespace\":\"Test\",\"name\":\"Test1\",\"fields\":["
        + "{\"name\":\"name\",\"type\":\"string\"},"
        + "{\"name\":\"opName\",\"type\":\"optional\",\"value\":\"string\"},"
        + "{\"name\":\"emptyOpName\",\"type\":\"optional\",\"value\":\"string\"},"
        + "{\"name\":\"opNameInt\",\"type\":\"optional\",\"value\":\"int\"},"
        + "{\"name\":\"emptyOpInt\",\"type\":\"optional\",\"value\":\"int\"},"
        + "{\"name\":\"name1\",\"type\":\"string\"}]}";

    final Parser parser = new Parser();
    final Schema schema = parser.parse(json);

    GenericRecord record = new Record(schema);
    record.put("name", "test");
    record.put("opName", Optional.of("testavro"));
    record.put("emptyOpName", Optional.empty());
    record.put("opNameInt", Optional.of(1234));
    record.put("emptyOpInt", Optional.empty());
    record.put("name1", "hola");

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


    // Verify
    Assert.assertEquals("test", readRecord.get("name").toString());
    Assert.assertEquals("hola", readRecord.get("name1").toString());

    final Optional<String> optional = (Optional<String>) readRecord.get("opName");
    Assert.assertTrue(optional.isPresent());
    Assert.assertEquals("testavro", optional.get());

    final Optional<Integer> optionalint = (Optional<Integer>) readRecord.get("opNameInt");
    Assert.assertTrue(optionalint.isPresent());
    Assert.assertEquals(1234, optionalint.get().intValue());

    final Optional<String> emptyOptional = (Optional<String>) readRecord.get("emptyOpName");
    Assert.assertFalse(emptyOptional.isPresent());

    final Optional<Integer> emptyOptionalInt = (Optional<Integer>) readRecord.get("emptyOpInt");
    Assert.assertFalse(emptyOptionalInt.isPresent());

  }

  @SuppressWarnings("unchecked")
  @Test
  public void testWriteAndReadWithOptionalResolution() throws IOException {
    String writeJson = "{\"type\":\"record\",\"namespace\":\"Test\",\"name\":\"Test1\",\"fields\":["
        + "{\"name\":\"name\",\"type\":\"string\"},"
        + "{\"name\":\"opName\",\"type\":\"optional\",\"value\":\"string\"},"
        + "{\"name\":\"emptyOpName\",\"type\":\"optional\",\"value\":\"string\"},"
        + "{\"name\":\"opNameInt\",\"type\":\"optional\",\"value\":\"int\"},"
        + "{\"name\":\"emptyOpInt\",\"type\":\"optional\",\"value\":\"int\"},"
        + "{\"name\":\"name1\",\"type\":\"string\"}]}";

    final Parser parser = new Parser();
    final Schema writerSchema = parser.parse(writeJson);

    GenericRecord record = new Record(writerSchema);
    record.put("name", "test");
    record.put("opName", Optional.of("testavro"));
    record.put("emptyOpName", Optional.empty());
    record.put("opNameInt", Optional.of(1234));
    record.put("emptyOpInt", Optional.empty());
    record.put("name1", "hola");

    // Write
    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(writerSchema);
    Encoder encoder = EncoderFactory.get().binaryEncoder(outStream, null);
    writer.write(record, encoder);
    encoder.flush();
    outStream.close();



    String readJson = "{\"type\":\"record\",\"namespace\":\"Test\",\"name\":\"Test1\",\"fields\":["
        + "{\"name\":\"name\",\"type\":\"string\"},"
        + "{\"name\":\"opName\",\"type\":\"optional\",\"value\":\"string\"},"
        + "{\"name\":\"emptyOpName\",\"type\":\"optional\",\"value\":\"string\"},"
        + "{\"name\":\"opNameInt\",\"type\":\"optional\",\"value\":\"long\"},"
        + "{\"name\":\"emptyOpInt\",\"type\":\"optional\",\"value\":\"int\"},"
        + "{\"name\":\"name1\",\"type\":\"string\"}]}";

    final Schema readerSchema = new Parser().parse(readJson);

    // Read
    DatumReader<GenericRecord> reader = new GenericDatumReader<>(writerSchema,readerSchema);
    final Decoder binaryDecoder = DecoderFactory.get().binaryDecoder(outStream.toByteArray(), null);
    final GenericRecord readRecord = reader.read(null, binaryDecoder);


    // Verify
    Assert.assertEquals("test", readRecord.get("name").toString());
    Assert.assertEquals("hola", readRecord.get("name1").toString());

    final Optional<String> optional = (Optional<String>) readRecord.get("opName");
    Assert.assertTrue(optional.isPresent());
    Assert.assertEquals("testavro", optional.get());

    final Optional<Long> optionalLong = (Optional<Long>) readRecord.get("opNameInt");
    Assert.assertTrue(optionalLong.isPresent());
    Assert.assertEquals(1234, optionalLong.get().longValue());

    final Optional<String> emptyOptional = (Optional<String>) readRecord.get("emptyOpName");
    Assert.assertFalse(emptyOptional.isPresent());

    final Optional<Integer> emptyOptionalInt = (Optional<Integer>) readRecord.get("emptyOpInt");
    Assert.assertFalse(emptyOptionalInt.isPresent());

  }


}
