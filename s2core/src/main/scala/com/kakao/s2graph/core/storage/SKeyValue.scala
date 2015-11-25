package com.kakao.s2graph.core.storage

import org.apache.hadoop.hbase.util.Bytes
import org.hbase.async.KeyValue

object SKeyValue {
  private def fromWithLen(bytes: Array[Byte], offset: Int): (Array[Byte], Int) = {
    val len = Bytes.toInt(bytes, offset, 4)
    val endAt = offset + 4 + len
    (bytes.slice(offset + 4, endAt), endAt)
  }
  def fromBytes(bytes: Array[Byte], offset: Int): (SKeyValue, Int) = {
    val (table, tableEndAt) = fromWithLen(bytes, offset)
    val (row, rowEndAt) = fromWithLen(bytes, tableEndAt)
    val (cf, cfEndAt) = fromWithLen(bytes, rowEndAt)
    val (qualifier, qualifierEndAt) = fromWithLen(bytes, cfEndAt)
    val (value, valueEndAt) = fromWithLen(bytes, qualifierEndAt)
    val timestamp = Bytes.toLong(bytes, valueEndAt, 8)
    val endAt = offset +
      table.length + row.length + cf.length + qualifier.length + value.length + 8 +
      5 * 4
    (SKeyValue(table, row, cf, qualifier, value, timestamp), endAt)
  }
}
case class SKeyValue(table: Array[Byte], row: Array[Byte], cf: Array[Byte],
                     qualifier: Array[Byte], value: Array[Byte], timestamp: Long) {
  private def withLen(b: Array[Byte]): Array[Byte] = Bytes.add(Bytes.toBytes(b.length), b)

  def bytes = {
    val tableBytes = withLen(table)
    val rowBytes = withLen(row)
    val cfBytes = withLen(cf)
    val qualifierBytes = withLen(qualifier)
    val valueBytes = withLen(value)
    val timestampBytes = Bytes.toBytes(timestamp)

    Bytes.add(Bytes.add(Bytes.add(tableBytes, rowBytes, cfBytes),
      qualifierBytes, valueBytes), timestampBytes)
  }

  def toLogString = {
    Map("table" -> table.toList, "row" -> row.toList, "cf" -> cf.toList, "qualifier" -> qualifier.toList, "value" -> value.toList, "timestamp" -> timestamp).toString
  }
}

trait CanSKeyValue[T] {
  def toSKeyValue(from: T): SKeyValue
}

object CanSKeyValue {



  // For asyncbase KeyValues
  implicit val asyncKeyValue = new CanSKeyValue[KeyValue] {
    def toSKeyValue(kv: KeyValue): SKeyValue = {
      SKeyValue(Array.empty[Byte], kv.key(), kv.family(), kv.qualifier(), kv.value(), kv.timestamp())
    }
  }

  // For asyncbase KeyValues
  implicit val sKeyValue = new CanSKeyValue[SKeyValue] {
    def toSKeyValue(kv: SKeyValue): SKeyValue = kv
  }

  // For hbase KeyValues
}

