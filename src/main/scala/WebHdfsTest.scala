import java.io.FileInputStream
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem
import org.apache.hadoop.io.IOUtils

/**
  * Created by Administrator on 2016/9/10.
  */
object WebHdfsTest {

 def main(args: Array[String]) {

  System.setProperty("HADOOP_USER_NAME", "Administrator")

  val conf = new Configuration

//  val fs = new WebHdfsFileSystem
//  fs.initialize(URI.create("Webhdfs://localhost:50070"), conf)
  val fs = FileSystem.get(URI.create("webhdfs://localhost:50070"), conf)


  if (!fs.exists(new Path("/user/joe"))) {
   fs.mkdirs(new Path("/user/joe"))
  }
  import scala.collection.convert._
  val status = fs.listStatus(new Path("/user/joe"))

  status.foreach(println)

  val localIS = new FileInputStream("e:/spark/README.md")

  val os = fs.create(new Path("/user/joe/README.md"))
  IOUtils.copyBytes(localIS, os, 1024, true)

  val is = fs.open(new Path("/user/joe/README.md"))
  IOUtils.copyBytes(is, System.out, 1024, true)
 }
}
