package com.digitalchina.dcn.hadoop;

import com.digitalchina.dcn.hadoop.avro.StringPair;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.util.Utf8;
import org.apache.commons.io.output.ByteArrayOutputStream;

import java.io.File;
import java.io.IOException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * Created by Administrator on 2016/9/13 0013.
 */
public class AvroTest {

    public static void main(String[] args) throws IOException {

        genericAvroTest();

        specificAvroTest();

        fileAvroTest();
    }

    private static void fileAvroTest() throws IOException {
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(AvroTest.
                class.getClassLoader().getResourceAsStream("StringPair.avsc"));

        GenericRecord datum = new GenericData.Record(schema);
        datum.put("left", "joe");
        datum.put("right", "xia xue");

        File file = new File("data.avro");
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> dataFileWriter =
                new DataFileWriter<GenericRecord>(writer);
        dataFileWriter.create(schema, file);
        dataFileWriter.append(datum);
        dataFileWriter.close();

        DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();
        DataFileReader<GenericRecord> dataFileReader =
                new DataFileReader<GenericRecord>(file, reader);
        assertThat("schema is the same", schema , is(dataFileReader.getSchema()));

        GenericRecord record = null;
        while (dataFileReader.hasNext()) {
            record = dataFileReader.next(record);
            assertThat(record.get("left").toString(), is("joe"));
            assertThat(record.get("right").toString(), is("xia xue"));
        }
    }


    private static void specificAvroTest() throws IOException {
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(AvroTest.class.getClassLoader().getResourceAsStream("StringPair.avsc"));

        StringPair datum = new StringPair();
        datum.setLeft("left hand");
        datum.setRight("right hand");

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DatumWriter<StringPair> writer = new SpecificDatumWriter<StringPair>(schema);
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        writer.write(datum, encoder);
        encoder.flush();
        out.close();

        DatumReader<StringPair> reader = new SpecificDatumReader<StringPair>(schema);
        Decoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);
        StringPair result = reader.read(null, decoder);
        assertThat(result.getLeft().toString(), is("left hand"));
        assertThat(result.getRight().toString(), is("right hand"));

    }

    private static void genericAvroTest() throws IOException {
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(AvroTest.class.getClassLoader().getResourceAsStream("StringPair.avsc"));

        Schema schemaNew = parser.parse(AvroTest.class.getClassLoader().getResourceAsStream("StringPair-2.0.avsc"));

        GenericRecord record = new GenericData.Record(schema);
        record.put("left", new Utf8("left hand"));
        record.put("right", new Utf8("right hand"));

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        writer.write(record, encoder);
        encoder.flush();
        out.close();

        DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema, schemaNew);
        Decoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);
        GenericRecord result = reader.read(null, decoder);
        assertThat(result.get("left").toString(), is("left hand"));
        assertThat(result.get("right").toString(), is("right hand"));


        Schema schemaDelete = parser.parse(AvroTest.class.getClassLoader().getResourceAsStream("StringPair-delete.avsc"));
        reader = new GenericDatumReader<GenericRecord>(schema, schemaDelete);
        decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);
        result = reader.read(null, decoder);
        assertThat(result.get("left").toString(), is("left hand"));
    }
}
