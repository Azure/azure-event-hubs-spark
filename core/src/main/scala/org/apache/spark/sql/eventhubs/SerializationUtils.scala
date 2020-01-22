package org.apache.spark.sql.eventhubs

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.util.Base64

import org.apache.spark.internal.Logging

object SerializationUtils extends Logging{

  def serialize(obj: Any): String = {
    val bo = new ByteArrayOutputStream
    val so = new ObjectOutputStream(bo)
    so.writeObject(obj)
    so.flush()
    val bytes = bo.toByteArray
    logInfo(s"Serialized object size: $bytes bytes")
    val sizeLimitInMB = 1
    require(bytes.size < sizeLimitInMB * 1024 * 1024, s"Serialized object size is $bytes bytes, which is larger than limit $sizeLimitInMB MB")
    Base64.getEncoder.encodeToString(bytes)
  }

  def deserialize[T](serialized: String): T = {
    val bi = new ByteArrayInputStream(
      Base64.getDecoder.decode(serialized))
    val si = new ObjectInputStream(bi)
    si.readObject.asInstanceOf[T]
  }
}
