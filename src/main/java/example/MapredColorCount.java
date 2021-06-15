package example;

import java.io.IOException;
import java.net.URI;

import org.apache.avro.*;
import org.apache.avro.Schema.Type;
import org.apache.avro.mapred.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import example.avro.User;

public class MapredColorCount extends Configured implements Tool {

    public static class ColorCountMapper extends AvroMapper<User, Pair<CharSequence, Integer>> {
        @Override
        public void map(User user, AvroCollector<Pair<CharSequence, Integer>> collector, Reporter reporter)
                throws IOException {
            CharSequence color = user.getFavoriteColor();
            // We need this check because the User.favorite_color field has type ["string", "null"]
            if (color == null) {
                color = "none";
            }
            collector.collect(new Pair<CharSequence, Integer>(color, 1));
        }
    }

    public static class ColorCountReducer extends AvroReducer<CharSequence, Integer,
            Pair<CharSequence, Integer>> {
        @Override
        public void reduce(CharSequence key, Iterable<Integer> values,
                           AvroCollector<Pair<CharSequence, Integer>> collector,
                           Reporter reporter)
                throws IOException {
            int sum = 0;
            for (Integer value : values) {
                sum += value;
            }
            collector.collect(new Pair<CharSequence, Integer>(key, sum));
        }
    }

    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: MapredColorCount <input path> <output path>");
            return -1;
        }

        System.setProperty("HADOOP_USER_NAME", "root");
        FileSystem fs = FileSystem.get(URI.create("hdfs://master:9000"), new Configuration());
        if (fs.exists(new Path("/user/root/output"))) {
            fs.delete(new Path("/user/root/output"), true);
        }


        JobConf conf = new JobConf(getConf(), MapredColorCount.class);
        //设置jar

        conf.setJar("./target/hadoop-test-1.0-SNAPSHOT.jar");
        conf.set("fs.defaultFS", "hdfs://master:9000/");
        conf.set("hadoop.job.user","joe");
        //指定jobtracker的ip和端口号，master在/etc/hosts中可以配置
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("mapreduce.jobtracker.address", "master:9001");
        conf.set("yarn.resourcemanager.hostname", "master");
        conf.set("yarn.resourcemanager.admin.address", "master:8033");
        conf.set("yarn.resourcemanager.address", "master:8032");
        conf.set("yarn.resourcemanager.resource-tracker.address", "master:8035");
        conf.set("yarn.resourcemanager.scheduler.address", "master:8030");
        conf.setJobName("colorcount");

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        AvroJob.setMapperClass(conf, ColorCountMapper.class);
        AvroJob.setReducerClass(conf, ColorCountReducer.class);

        // Note that AvroJob.setInputSchema and AvroJob.setOutputSchema set
        // relevant config options such as input/output format, map output
        // classes, and output key class.
        AvroJob.setInputSchema(conf, User.getClassSchema());
        AvroJob.setOutputSchema(conf, Pair.getPairSchema(Schema.create(Type.STRING),
                Schema.create(Type.INT)));

        JobClient.runJob(conf);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new MapredColorCount(), args);
        System.exit(res);
    }
}