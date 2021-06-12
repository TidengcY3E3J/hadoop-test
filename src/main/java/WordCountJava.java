import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.net.URI;

/**
 * Created by Administrator on 2016/9/9.
 */
public class WordCountJava {

    private static class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {
        private static final IntWritable one = new IntWritable(1);
        private static final Text wordText = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] words = line.split(" ");

            for (String word : words) {
                wordText.set(word);
                context.write(wordText, one);
            }
        }
    }

    private static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }

            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length != 2) {
            System.exit(-1);
        }

        System.setProperty("HADOOP_USER_NAME", "root");

        JobConf conf = new JobConf();
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


        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }

        FileSystem fs = FileSystem.get(URI.create("hdfs://master:9000"), conf);

        if (fs.exists(new Path("/user/fkong/output"))) {
            fs.delete(new Path("/user/fkong/output"), true);
        }

        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCountJava.class);
        job.setMapperClass(WordCountMapper.class);
        job.setCombinerClass(WordCountReducer.class);
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));

        // 压缩属性配置
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
