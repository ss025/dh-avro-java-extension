package in.dailyhunt.avro;

import static org.junit.Assert.assertEquals;

import in.dailyhunt.avro.Schema.Parser;
import in.dailyhunt.avro.file.DataFileReader;
import in.dailyhunt.avro.generic.GenericData.Record;
import in.dailyhunt.avro.generic.GenericDatumReader;
import in.dailyhunt.avro.generic.GenericDatumWriter;
import in.dailyhunt.avro.generic.GenericRecord;
import in.dailyhunt.avro.io.DatumWriter;
import in.dailyhunt.avro.io.Encoder;
import in.dailyhunt.avro.io.EncoderFactory;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import org.junit.Test;

public class UnionRecordTest {


  private static final Schema FULL_SCHEMA = new Parser().parse(readResource("/union/union_record_full_schema.json"));
  private static final Schema VIDEO_SCHEMA = new Parser().parse(readResource("/union/video_asset.json"));
  private static final Schema IMAGE_SCHEMA = new Parser().parse(readResource("/union/image_asset.json"));

  private static String readResource(String name){
    try {
      return new String(Files.readAllBytes(Paths.get(UnionRecordTest.class.getResource(name).toURI())));
    }catch (Exception e){
      throw new RuntimeException(e);
    }
  }

  /**
   * This is need to test compatibility with other languages read .
   *
   */
  @Test
  public void writeUnionRecordBytesOnly() throws Exception {

    GenericRecord imageRecord = new Record(IMAGE_SCHEMA);
    imageRecord.put("asset_uid",1000001);
    imageRecord.put("asset_type",9999999);

    GenericRecord record = new Record(FULL_SCHEMA);
    record.put("asset_source","123456-Src");
    record.put("asset_type", imageRecord);


    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(FULL_SCHEMA);
    Encoder encoder = EncoderFactory.get().binaryEncoder(outStream, null);
    writer.write(record, encoder);
    encoder.flush();
    outStream.close();

    // write raw bytes to file
    try (OutputStream outputStream = new FileOutputStream("/tmp/union-r.avro")) {
      outStream.writeTo(outputStream);
      outputStream.flush();
    }

    outStream.close();

    System.out.println("Byte Array Len = "+ outStream.toByteArray().length);

    System.out.println("Byte Array     = "+ Arrays.toString(outStream.toByteArray()));


  }


  /**
   * This is need to test compatibility with other languages read .
   *
   */
  @Test
  public void readUnionRecordBytesOnly() throws Exception {
    final String avroFile = "/tmp/union-w.avro";
    GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(FULL_SCHEMA, FULL_SCHEMA);
    DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(new File(avroFile), reader);

    final GenericRecord record = dataFileReader.next();
    System.out.println("Record read is "+ record);
    assertEquals("1234-src", record.get("asset_source").toString());

  }

}
