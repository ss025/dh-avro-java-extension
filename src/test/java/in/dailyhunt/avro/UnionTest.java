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
import java.util.Optional;
import org.junit.Assert;
import org.junit.Test;

public class UnionTest {

  @Test
  public void testUnionSchema() throws IOException {

    String schema = "{\n"
        + "    \"namespace\": \"proj\",\n"
        + "    \"doc\" : \"New Type Test\",\n"
        + "    \"name\": \"newType\",\n"
        + "    \"type\" : \"record\",\n"
        + "    \"fields\": [\n"
        + "      {\n"
        + "        \"name\":\"BasicField\",\n"
        + "        \"type\":\"int\"\n"
        + "      },\n"
        + "    {\n"
        + "    \"name\": \"assetType\",\n"
        + "    \"type\": [\n"
        + "        {\n"
        + "            \"name\": \"video\",\n"
        + "            \"type\" : \"record\", \n"
        + "            \"fields\": \n"
        + "            [\n"
        + "                {\n"
        + "                    \"name\" : \"asset_id\", \n"
        + "                    \"type\" : \"int\"\n"
        + "                },\n"
        + "                {\n"
        + "                    \"name\" : \"asset_type\",\n"
        + "                    \"type\" : \"int\"\n"
        + "                }\n"
        + "            ]\n"
        + "        },\n"
        + "        {\n"
        + "            \"name\" : \"image\",\n"
        + "            \"type\" : \"record\",\n"
        + "            \"fields\" :\n"
        + "            [\n"
        + "                {\n"
        + "                    \"name\" : \"asset_uuid\",\n"
        + "                    \"type\" : \"int\"\n"
        + "                },\n"
        + "                {\n"
        + "                    \"name\" : \"asset_type\",\n"
        + "                    \"type\" : \"int\"\n"
        + "                }\n"
        + "            ]\n"
        + "        }\n"
        + "    ]\n"
        + "    }\n"
        + "    ]\n"
        + "}";

    System.out.println("Full schema "+ schema);


    String videoSchemaStr = "        {\n"
        +"          \"namespace\": \"proj\",\n"
        + "          \"name\": \"video\",\n"
        + "          \"type\": \"record\",\n"
        + "          \"fields\": [\n"
        + "            {\n"
        + "              \"name\": \"asset_id\",\n"
        + "              \"type\": \"int\"\n"
        + "            },\n"
        + "            {\n"
        + "              \"name\": \"asset_type\",\n"
        + "              \"type\": \"int\"\n"
        + "            }\n"
        + "          ]\n"
        + "        }";

    String imageSchemaStr = "        {\n"
        +"          \"namespace\": \"proj\",\n"
        + "          \"name\": \"image\",\n"
        + "          \"type\": \"record\",\n"
        + "          \"fields\": [\n"
        + "            {\n"
        + "              \"name\": \"asset_uuid\",\n"
        + "              \"type\": \"int\"\n"
        + "            },\n"
        + "            {\n"
        + "              \"name\": \"asset_type\",\n"
        + "              \"type\": \"int\"\n"
        + "            }\n"
        + "          ]\n"
        + "        }";

    final Schema videoSchema = new Parser().parse(videoSchemaStr);
    final Schema imageSchema = new Parser().parse(imageSchemaStr);
    final Schema parse = new Parser().parse(schema);
    System.out.println("Schema is "+parse);
    Assert.assertNotNull(parse);
    Assert.assertTrue(parse.getClass().getSimpleName().equalsIgnoreCase("RecordSchema"));


    GenericRecord videoRecord = new Record(imageSchema);
    videoRecord.put("asset_uuid",1);
    videoRecord.put("asset_type",2);

    GenericRecord record = new Record(parse);
    record.put("BasicField", 1);
    record.put("assetType", videoRecord);

    // Write
    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(parse);
    Encoder encoder = EncoderFactory.get().binaryEncoder(outStream, null);
    writer.write(record, encoder);
    encoder.flush();
    outStream.close();

    // Read
    DatumReader<GenericRecord> reader = new GenericDatumReader<>(parse);
    final Decoder binaryDecoder = DecoderFactory.get().binaryDecoder(outStream.toByteArray(), null);
    final GenericRecord readRecord = reader.read(null, binaryDecoder);
    System.out.println("=======================");
    System.out.println("Read record "+readRecord);


  }

}
