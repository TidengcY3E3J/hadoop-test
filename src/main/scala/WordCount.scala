import java.lang.Iterable
import java.net.URI

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.util.GenericOptionsParser

import scala.collection.JavaConversions._
/**
  * Created by Administrator on 2016/9/9.
  */
object WordCount {

  private class ScalaWordCountMapper extends Mapper[LongWritable , Text, Text, IntWritable] {
    private final val one = new IntWritable(1)
    private final val wordText = new Text();

    def map(key: LongWritable , value: Text, context: Context) = {
      value.toString().split(" ").foreach {
        word =>
          wordText.set(word)
          context.write(wordText, one)
      }
    }
  }

    private class ScalaWordCountReducer extends Reducer[Text, IntWritable, Text, IntWritable] {
      private final val result = new IntWritable();

      def reduce(key: Text, values: Iterable[IntWritable], context: Context): Unit = {
        var sum = 0;
        values.foreach(sum += _.get())
        result.set(sum)
        context.write(key, result)
      }
    }

    def main(args: Array[String]) {
      System.setProperty("HADOOP_USER_NAME", "Administrator")

      val conf: JobConf = new JobConf
      //设置jar
      conf.setJar("./target/hadoop-test-1.0-SNAPSHOT.jar")
      conf.set("fs.defaultFS", "hdfs://localhost:9000/")
      conf.set("hadoop.job.user", "joe")
      //指定jobtracker的ip和端口号，master在/etc/hosts中可以配置
      conf.set("mapreduce.framework.name", "local")
      conf.set("mapreduce.jobtracker.address", "localhost:9001")
      conf.set("yarn.resourcemanager.hostname", "localhost")
      conf.set("yarn.resourcemanager.admin.address", "localhost:8033")
      conf.set("yarn.resourcemanager.address", "localhost:8032")
      conf.set("yarn.resourcemanager.resource-tracker.address", "localhost:8035")
      conf.set("yarn.resourcemanager.scheduler.address", "localhost:8030")

      val otherArgs: Array[String] = new GenericOptionsParser(conf, args).getRemainingArgs
      if (otherArgs.length < 2) {
        System.err.println("Usage: wordcount <in> [<in>...] <out>")
        System.exit(2)
      }

      val fs: FileSystem = FileSystem.get(URI.create("hdfs://localhost:9000"), conf)

      if (fs.exists(new Path("/user/fkong/output"))) {
        fs.delete(new Path("/user/fkong/output"), true)
      }

      val job: Job = Job.getInstance(conf, "word count")
      job.setJarByClass(WordCount.getClass)
      job.setMapperClass(classOf[ScalaWordCountMapper])
      job.setCombinerClass(classOf[ScalaWordCountReducer])
      job.setReducerClass(classOf[ScalaWordCountReducer])
      job.setOutputKeyClass(classOf[Text])
      job.setOutputValueClass(classOf[IntWritable])

      for (index <- 0 until otherArgs.length - 1) {
        FileInputFormat.addInputPath(job, new Path(otherArgs(index)))
      }

      FileOutputFormat.setOutputPath(job, new Path(otherArgs(otherArgs.length - 1)))
      System.exit(if (job.waitForCompletion(true)) 0
      else 1)
    }
}
