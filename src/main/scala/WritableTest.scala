import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import org.apache.hadoop.io.{IntWritable, VIntWritable, Writable}
import org.apache.hadoop.util.StringUtils

/**
  * Created by Administrator on 2016/9/12 0012.
  */
object WritableTest {



 def serialize(writable: Writable) = {
  val out = new ByteArrayOutputStream();
  val dataOut = new DataOutputStream(out)

  writable.write(dataOut)
  dataOut.close();

  out.toByteArray
 }

 def deserialize(writable: Writable, bytes: Array[Byte]) = {
  val in = new ByteArrayInputStream(bytes)
  val dataInput = new DataInputStream(in)

  writable.readFields(dataInput)

  dataInput.close()
  bytes
 }

 def main(args: Array[String]) {
  val intWritable = new VIntWritable(-199)

  val bytes = serialize(intWritable)

  println(StringUtils.byteToHexString(bytes))

  val deserializeInt = new VIntWritable()
  deserialize(deserializeInt, bytes)

  println(deserializeInt.get())
 }
}
