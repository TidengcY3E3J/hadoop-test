import java.io.FileInputStream
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path, PathFilter}
import org.apache.hadoop.io.IOUtils
import org.apache.hadoop.util.Progressable

/**
  * Created by Administrator on 2016/9/8 0008.
  */
object HdfsTest {

  def main(args: Array[String]) {
    // 设置Hadoop 用户名
    System.setProperty("HADOOP_USER_NAME", "Administrator")

    val uri = "hdfs://localhost:9000"
    val config = new Configuration()
    val fs = FileSystem.get(URI.create(uri), config)

    if (!fs.exists(new Path("/user/fkong"))) {
      fs.mkdirs(new Path("/user/fkong"))
    }
    import scala.collection.convert._
    val status = fs.listStatus(new Path("/user/fkong"))

    status.foreach(println)

    val localIS = new FileInputStream("e:/spark/README.md")

    val os = fs.create(new Path("/user/fkong/README.md"), new Progressable {
      override def progress(): Unit = println(".")
    })
    IOUtils.copyBytes(localIS, os, 1024, false)
    os.hflush()

    assert(fs.getFileStatus(new Path("/user/fkong/README.md")).getLen != 0)

    fs.globStatus(new Path("/user/*"), new PathFilter {
      override def accept(path: Path): Boolean = {
        !path.toString.matches("^.*/user/fkong$")
      }
    }).foreach(println)

//    val is = fs.open(new Path("/user/fkong/README.md"))
//    IOUtils.copyBytes(is, System.out, 1024, true)
  }

}
