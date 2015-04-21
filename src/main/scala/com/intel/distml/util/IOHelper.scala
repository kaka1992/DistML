package com.intel.distml.util

import java.io.{DataInputStream, File, FileOutputStream, ObjectOutputStream}
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object IOHelper {
  def readString(dis: DataInputStream): String = {
    dis.readUTF()
  }

  def readInt(dis: DataInputStream): Int = {
    while (dis.available() < 4)
      Thread.sleep(1)

    dis.readInt()
  }

  def deleteHDFS(path: String) {
    val conf = new Configuration()
    val p = URI.create(path)
    val fs = FileSystem.get(p, conf)
    val dst = new Path(path)
    fs.delete(dst, true)
  }

  def writeToTemp(obj: AnyRef): Unit = {
    val f = new File("/tmp/scaml/")
    if (!f.exists()) {
      f.mkdirs()
    }

    for (i <- 1 until 1000; f1 = new File(f, "" + i)) if (!f1.exists) {
      println("write obj: " + obj)
      val os = new ObjectOutputStream(new FileOutputStream(f1))
      os.writeObject(obj)
      os.close()
    }
  }

}
