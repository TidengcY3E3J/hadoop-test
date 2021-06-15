package example;

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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * Created by Administrator on 2016/9/13 0013.
 */
public class GenerateUser {

    public static void main(String[] args) throws IOException {

        System.setProperty("HADOOP_USER_NAME", "root");

        fileAvroTest();
    }

    private static void fileAvroTest() throws IOException {
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(GenerateUser.
                class.getClassLoader().getResourceAsStream("User.avsc"));

        GenericRecord datum = new GenericData.Record(schema);
        datum.put("name", "joe");
        datum.put("favorite_number", 3);
        datum.put("favorite_color", "red");

        File file = new File("user.avro");
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> dataFileWriter =
                new DataFileWriter<GenericRecord>(writer);
//        dataFileWriter.create(schema, file);

        FileSystem fs = FileSystem.get(URI.create("hdfs://master:9000"), new Configuration());
        FSDataOutputStream output = fs.create(new Path("/user/joe/user.avro"));
        dataFileWriter.create(schema, output);

        dataFileWriter.append(datum);

        datum.put("name", "alfa");
        datum.put("favorite_number", 2);
        datum.put("favorite_color", "green");

        dataFileWriter.append(datum);

        datum.put("name", "lilei");
        datum.put("favorite_number", 11);
        datum.put("favorite_color", "");

        dataFileWriter.append(datum);

        datum.put("name", "xia");
        datum.put("favorite_number", 1);
        datum.put("favorite_color", "blue");

        dataFileWriter.append(datum);

        datum.put("name", "brother");
        datum.put("favorite_number", 23);
        datum.put("favorite_color", "red");

        dataFileWriter.append(datum);

        dataFileWriter.close();

        DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();
        DataFileReader<GenericRecord> dataFileReader =
                new DataFileReader<GenericRecord>(file, reader);
        assertThat("schema is the same", schema , is(dataFileReader.getSchema()));

        GenericRecord record = null;
        while (dataFileReader.hasNext()) {
            record = dataFileReader.next(record);
        }
    }

}
