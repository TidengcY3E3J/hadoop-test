package com.digitalchina.dcn.hadoop

import java.lang.Iterable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.util.GenericOptionsParser

import scala.collection.JavaConversions._


/**
  * Created by Administrator on 2016/9/8 0008.
  */
object WordCount {

  private[hadoop] class WordCountMapper extends Mapper[Object, Text, Text, IntWritable] {
    private final val one = new IntWritable(1)
    private final val event = new Text()

    def map(key: Object, value: Text, context: Context): Unit = {
      val words = value.toString.split(" ")
      words.foreach{ word =>
        event.set(word)
        context.write(event, one)
      }
    }
  }

  private[hadoop] class WordCountReducer extends Reducer[Text, IntWritable, Text, IntWritable] {
    private final val result = new IntWritable()

    def reduce(key: Text, values: Iterable[IntWritable], context: Context): Unit = {
      var sum = 0;
      values.foreach(sum += _.get())
      result.set(sum)
      context.write(key, result)
    }
  }

  def main(args: Array[String]) {
    val conf = new Configuration()
    val otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: EventCount <in> <out>");
      System.exit(2);
    }
    val job = Job.getInstance(conf, "event count");
    job.setJarByClass(WordCount.getClass)
    job.setMapperClass(classOf[WordCountMapper]);
    job.setCombinerClass(classOf[WordCountReducer]);
    job.setReducerClass(classOf[WordCountReducer]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[IntWritable]);
    FileInputFormat.addInputPath(job, new Path(otherArgs(0)));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs(1)));
    System.exit {
      if (job.waitForCompletion(true))
        0
      else
        1
    }
  }
}
