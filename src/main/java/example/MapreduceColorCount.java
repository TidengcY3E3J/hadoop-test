package example;

import java.io.IOException;
import java.net.URI;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import example.avro.User;

public class MapReduceColorCount extends Configured implements Tool {

    public static class ColorCountMapper extends
            Mapper<AvroKey<User>, NullWritable, Text, IntWritable> {

        @Override
        public void map(AvroKey<User> key, NullWritable value, Context context)
                throws IOException, InterruptedException {

            CharSequence color = key.datum().getFavoriteColor();
            if (color == null) {
                color = "none";
            }
            context.write(new Text(color.toString()), new IntWritable(1));
        }
    }

    public static class ColorCountReducer extends
            Reducer<Text, IntWritable, AvroKey<CharSequence>, AvroValue<Integer>> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(new AvroKey<CharSequence>(key.toString()), new AvroValue<Integer>(sum));
        }
    }

    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: MapReduceColorCount <input path> <output path>");
            return -1;
        }
        System.setProperty("HADOOP_USER_NAME", "root");
        FileSystem fs = FileSystem.get(URI.create("hdfs://master:9000"), getConf());
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
        Job job = Job.getInstance(conf, "Color Count");
        job.setJarByClass(MapReduceColorCount.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setInputFormatClass(AvroKeyInputFormat.class);
        job.setMapperClass(ColorCountMapper.class);
        AvroJob.setInputKeySchema(job, User.getClassSchema());
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
        job.setReducerClass(ColorCountReducer.class);
        AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.STRING));
        AvroJob.setOutputValueSchema(job, Schema.create(Schema.Type.INT));

        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new MapReduceColorCount(), args);
        System.exit(res);
    }
}