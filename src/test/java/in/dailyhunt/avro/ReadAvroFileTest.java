package in.dailyhunt.avro;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import in.dailyhunt.avro.Schema.Parser;
import in.dailyhunt.avro.data.LruSet;
import in.dailyhunt.avro.data.LruValue;
import in.dailyhunt.avro.file.DataFileReader;
import in.dailyhunt.avro.generic.GenericData;
import in.dailyhunt.avro.generic.GenericData.Array;
import in.dailyhunt.avro.generic.GenericData.EnumSymbol;
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
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.Ignore;
import org.junit.Test;

public class ReadAvroFileTest {

  private static final Schema STATUS_SCHEMA = new Schema.Parser()
      .parse("{\"type\":\"enum\",\"name\":\"status\",\"symbols\":[\"A\",\"B\",\"C\"]}");


  @SuppressWarnings("unchecked")
  @Test
  public void readAvroFile() throws IOException {

    final String schemaJsonFile = System.getProperty("user.dir") + "/src/test/resources/schema.json";
    if (!new File(schemaJsonFile).exists()) {
      System.out.println("File does not exists at " + schemaJsonFile);
      return;
    }

    final String schemaJson = new String(Files.readAllBytes(Paths.get(schemaJsonFile)));
    final Schema schema = new Parser().parse(schemaJson);
    String avroFile = "/tmp/data-written.avro";
    if (!new File(avroFile).exists()) {
      System.out.println("Avro data file does not exists at " + avroFile);
      return;
    }

    GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema, schema);
    DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(new File(avroFile), reader);

    final GenericRecord record = dataFileReader.next();

    final int anInt = (int) record.get("int");
    final Utf8 anString = (Utf8) record.get("string");
    final EnumSymbol anEnum = (EnumSymbol) record.get("enum");
    final Array<Utf8> anArray = (Array<Utf8>) record.get("array");
    final Map<Utf8, Utf8> anMap = (Map<Utf8, Utf8>) record.get("map"); // value type will be from schema
    final Map<Utf8, EnumSymbol> anMap1 = (Map<Utf8, EnumSymbol>) record.get("map1"); // value type will be from schema
    final Set<Utf8> anSet = (Set<Utf8>) record.get("set");
    final LruSet anLruSet = (LruSet) record.get("lruSet");
    final Optional<String> anOptional = (Optional<String>) record.get("optionalStr"); // value type will be from schema

    assertEquals(123, anInt);
    assertEquals("nrtavro", anString.toString());
    assertEquals("A", anEnum.toString());
    assertEquals("arr1", anArray.get(0).toString());

    final Utf8 mapKey = new Utf8("key1");

    assertFalse("Map should not be empty", anMap.isEmpty());
    assertTrue("Map does not contain key ", anMap.containsKey(mapKey));
    assertEquals(new Utf8("value"), anMap.get(mapKey));

    assertFalse("Map1 should not be empty", anMap1.isEmpty());
    assertTrue("Map1 does not contain key ", anMap1.containsKey(mapKey));
    assertEquals("FOLLOW", anMap1.get(mapKey).toString());

    assertFalse(anSet.isEmpty());
    assertTrue(anSet.contains(new Utf8("hello")));

    assertFalse(anLruSet.isEmpty());
    assertTrue(anLruSet.containsKey("ig1"));

    assertFalse(anOptional.isPresent());

  }


  @Test
  public void writeAvroFile() throws IOException {

    final String schemaJsonFile = System.getProperty("user.dir") + "/src/test/resources/schema.json";
    if (!new File(schemaJsonFile).exists()) {
      System.out.println("File does not exists at " + schemaJsonFile);
      return;
    }

    final String schemaJson = new String(Files.readAllBytes(Paths.get(schemaJsonFile)));
    final Schema schema = new Parser().parse(schemaJson);

    GenericRecord record = new Record(schema);
    record.put("int", 123);
    record.put("string", "nrtavro");
    record.put("enum", new EnumSymbol(STATUS_SCHEMA, "A"));
    record.put("array", Collections.singletonList("arr1"));
    record.put("map", new HashMap<String, String>() {{
      put("key1", "value");
    }});
    record.put("map1", new HashMap<String, EnumSymbol>() {{
      put("key1", new EnumSymbol(STATUS_SCHEMA, "A"));
    }});

    record.put("set", new HashSet<>(Collections.singletonList("hello")));
    record.put("lruSet", new LruSet() {{
      put("ig1", new LruValue(123455, 22));
    }});

    record.put("optionalStr", Optional.empty());
    record.put("dateExample",System.currentTimeMillis());

    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
    Encoder encoder = EncoderFactory.get().binaryEncoder(outStream, null);
    writer.write(record, encoder);
    encoder.flush();

    // write raw bytes to file
    try (OutputStream outputStream = new FileOutputStream("foo.avro")) {
      outStream.writeTo(outputStream);
    }

    outStream.close();

    System.out.println(outStream.toByteArray().length);

    System.out.println(Arrays.toString(outStream.toByteArray()));
  }

}
