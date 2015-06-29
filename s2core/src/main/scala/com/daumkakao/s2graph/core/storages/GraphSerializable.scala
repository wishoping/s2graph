package com.daumkakao.s2graph.core.storages

import com.daumkakao.s2graph.core.models.{Label, LabelMeta, LabelIndex}
import com.daumkakao.s2graph.core._
import com.daumkakao.s2graph.core.types2._
import org.apache.hadoop.hbase.util.Bytes
import org.hbase.async.KeyValue
import play.api.Logger

/**
 * Created by shon on 6/28/15.
 */
object GraphStorable extends JSONParser {

  import com.daumkakao.s2graph.core.GraphConstant._

  trait Serializable[T, R] {
    def encode(from: T): R

    def decode(v: R): T

    /** common helpers for hbase serializable/deserializable */


    val EMPTY_SEQ_BYTE = Byte.MaxValue

    def labelOrderSeqWithIsInverted(labelOrderSeq: Byte, isInverted: Boolean): Array[Byte] = {
      assert(labelOrderSeq < (1 << 6))
      val byte = labelOrderSeq << 1 | (if (isInverted) 1 else 0)
      Array.fill(1)(byte.toByte)
    }

    def bytesToLabelIndexSeqWithIsInverted(bytes: Array[Byte], offset: Int): (Byte, Boolean) = {
      val byte = bytes(offset)
      val isInverted = if ((byte & 1) != 0) true else false
      val labelOrderSeq = byte >> 1
      (labelOrderSeq.toByte, isInverted)
    }

    def propsToBytes(props: Seq[(Byte, InnerValLike)]): Array[Byte] = {
      val len = props.length
      assert(len < Byte.MaxValue)
      var bytes = Array.fill(1)(len.toByte)
      for ((k, v) <- props) bytes = Bytes.add(bytes, v.bytes)
//      Logger.error(s"propsToBytes: $props => ${bytes.toList}")
      bytes
    }

    def propsToKeyValues(props: Seq[(Byte, InnerValLike)]): Array[Byte] = {
      val len = props.length
      assert(len < Byte.MaxValue)
      var bytes = Array.fill(1)(len.toByte)
      for ((k, v) <- props) bytes = Bytes.add(bytes, Array.fill(1)(k), v.bytes)
      bytes
    }

    def propsToKeyValuesWithTs(props: Seq[(Byte, InnerValLikeWithTs)]): Array[Byte] = {
      val len = props.length
      assert(len < Byte.MaxValue)
      var bytes = Array.fill(1)(len.toByte)
      for ((k, v) <- props) bytes = Bytes.add(bytes, Array.fill(1)(k), v.bytes)
      bytes
    }

    def bytesToKeyValues(bytes: Array[Byte],
                         offset: Int,
                         len: Int,
                         version: String): (Seq[(Byte, InnerValLike)], Int) = {
      var pos = offset
      val len = bytes(pos)
      pos += 1
      val kvs = for (i <- (0 until len)) yield {
        val k = bytes(pos)
        pos += 1
        val v = InnerVal.fromBytes(bytes, pos, 0, version)
        pos += v.bytes.length
        (k -> v)
      }
      val ret = (kvs.toList, pos)
      //    Logger.debug(s"bytesToProps: $ret")
      ret
    }

    def bytesToKeyValuesWithTs(bytes: Array[Byte],
                               offset: Int,
                               version: String): (Seq[(Byte, InnerValLikeWithTs)], Int) = {
      var pos = offset
      val len = bytes(pos)
      pos += 1
      val kvs = for (i <- (0 until len)) yield {
        val k = bytes(pos)
        pos += 1
        val v = InnerValLikeWithTs.fromBytes(bytes, pos, 0, version)
        pos += v.bytes.length
        (k -> v)
      }
      val ret = (kvs.toList, pos)
      //    Logger.debug(s"bytesToProps: $ret")
      ret
    }

    def bytesToProps(bytes: Array[Byte],
                     offset: Int,
                     version: String): (Seq[(Byte, InnerValLike)], Int) = {
//      Logger.error(s"${bytes.toList}, $offset, $version")
      var pos = offset
      val len = bytes(pos)
      pos += 1
      val kvs = for (i <- (0 until len)) yield {
        val k = EMPTY_SEQ_BYTE
        val v = InnerVal.fromBytes(bytes, pos, 0, version)
        pos += v.bytes.length
        (k -> v)
      }
//      Logger.error(s"bytesToProps: $kvs")
      val ret = (kvs.toList, pos)

      ret
    }
  }

  def defaultLabelPropsWithTs(label: Label, propsWithTs: Map[Byte, InnerValLikeWithTs]) = {
    val labelMetas = LabelMeta.findAllByLabelId(label.id.get)
    val propsWithDefault = (for (meta <- labelMetas) yield {
      propsWithTs.get(meta.seq) match {
        case Some(v) => (meta.seq -> v)
        case None =>
          val defaultInnerVal = toInnerVal(meta.defaultValue, meta.dataType, label.schemaVersion)
          (meta.seq -> InnerValLikeWithTs(defaultInnerVal, 0L))
      }
    }).toMap
    propsWithDefault
  }

  def filterEdge(edge: Edge, queryParam: QueryParam) = {
    val defaults = defaultLabelPropsWithTs(edge.label, edge.propsWithTs)
    val matches =
      for {
        (k, v) <- queryParam.hasFilters
        edgeVal <- defaults.get(k) if edgeVal.innerVal == v
      } yield (k -> v)

    matches.size == queryParam.hasFilters.size && queryParam.where.map(_.filter(edge)).getOrElse(true)
  }

  /** public interface */
  def toKeyValue(edgeWithIndex: EdgeWithIndex): List[KeyValue] = {
    val kvs = edgeWithIndex.schemaVer match {
      case InnerVal.VERSION1 => GraphStorable.IndexedEdgeLikeV1.encode(edgeWithIndex)
      case InnerVal.VERSION2 => GraphStorable.IndexedEdgeLikeV2.encode(edgeWithIndex)
      case InnerVal.VERSION3 => GraphStorable.IndexedEdgeLikeV3.encode(edgeWithIndex)
    }
    kvs.toList
  }

  def toKeyValue(edgeWithIndexInverted: EdgeWithIndexInverted): List[KeyValue] = {
    val kvs = edgeWithIndexInverted.schemaVer match {
      case InnerVal.VERSION1 => GraphStorable.SnapshotEdgeLikeV1.encode(edgeWithIndexInverted)
      case InnerVal.VERSION2 => GraphStorable.SnapshotEdgeLikeV2.encode(edgeWithIndexInverted)
      case InnerVal.VERSION3 => GraphStorable.SnapshotEdgeLikeV3.encode(edgeWithIndexInverted)
    }
    kvs.toList
  }
  def startKey(edgeWithIndex: EdgeWithIndex): Array[Byte] = {
    edgeWithIndex.schemaVer match {
      case InnerVal.VERSION1 => Array.empty
      case InnerVal.VERSION2 => Array.empty
      case InnerVal.VERSION3 => GraphStorable.IndexedEdgeLikeV3.startKey(edgeWithIndex)
    }
  }
  def stopKey(edgeWithIndex: EdgeWithIndex): Array[Byte] = {
    edgeWithIndex.schemaVer match {
      case InnerVal.VERSION1 => Array.empty
      case InnerVal.VERSION2 => Array.empty
      case InnerVal.VERSION3 => GraphStorable.IndexedEdgeLikeV3.stopKey(edgeWithIndex)
    }
  }
  def startKey(edgeWithIndexInverted: EdgeWithIndexInverted): Array[Byte] = {
    edgeWithIndexInverted.schemaVer match {
      case InnerVal.VERSION1 => Array.empty
      case InnerVal.VERSION2 => Array.empty
      case InnerVal.VERSION3 => GraphStorable.SnapshotEdgeLikeV3.startKey(edgeWithIndexInverted)
    }
  }
  def stopKey(edgeWithIndexInverted: EdgeWithIndexInverted): Array[Byte] = {
    edgeWithIndexInverted.schemaVer match {
      case InnerVal.VERSION1 => Array.empty
      case InnerVal.VERSION2 => Array.empty
      case InnerVal.VERSION3 => GraphStorable.SnapshotEdgeLikeV3.stopKey(edgeWithIndexInverted)
    }
  }
  def toSnapshotEdge(kvs: Seq[KeyValue], queryParam: QueryParam): Option[Edge] = {
    val edge: Edge = queryParam.label.schemaVersion match {
      case InnerVal.VERSION1 => GraphStorable.SnapshotEdgeLikeV1.decode(kvs).toEdge
      case InnerVal.VERSION2 => GraphStorable.SnapshotEdgeLikeV2.decode(kvs).toEdge
      case InnerVal.VERSION3 => GraphStorable.SnapshotEdgeLikeV3.decode(kvs).toEdge
    }
    if (!filterEdge(edge, queryParam)) None
    else Option(edge)
  }

  def toIndexedEdge(kvs: Seq[KeyValue], queryParam: QueryParam): Option[Edge] = {
//    Logger.debug(s"decoded: $kvs")
    val edge = queryParam.label.schemaVersion match {
      case InnerVal.VERSION1 => Option(GraphStorable.IndexedEdgeLikeV1.decode(kvs).toEdge)
      case InnerVal.VERSION2 => Option(GraphStorable.IndexedEdgeLikeV2.decode(kvs).toEdge)
      case InnerVal.VERSION3 =>
        //        None
        Option(GraphStorable.IndexedEdgeLikeV3.decode(kvs).toEdge)
    }
    edge
  }


  /** implementations */
  object IndexedEdgeLikeV1 extends Serializable[EdgeWithIndex, Seq[KeyValue]] {


    val version = InnerVal.VERSION1

    def encode(edgeWithIndex: EdgeWithIndex): Seq[KeyValue] = {
      val id = VertexId.toSourceVertexId(edgeWithIndex.srcVertex.id)
      /** rowKey */
      val rowKey = Bytes.add(id.bytes,
        edgeWithIndex.labelWithDir.bytes,
        labelOrderSeqWithIsInverted(edgeWithIndex.labelIndexSeq,
          edgeWithIndex.isInverted))

      /** qualifier */
      val tgtVertexIdBytes = VertexId.toTargetVertexId(edgeWithIndex.tgtVertex.id).bytes
      val idxPropsMap = edgeWithIndex.orders.toMap
      val idxPropsBytes = propsToBytes(edgeWithIndex.orders)

      val qualifier = Bytes.add(idxPropsBytes, tgtVertexIdBytes, Array[Byte](edgeWithIndex.op))
      /** value */
      val value = propsToKeyValues(edgeWithIndex.metas.toList)

      Seq(new KeyValue(rowKey, edgeCf, qualifier, edgeWithIndex.ts, value))
    }

    def decode(kvs: Seq[KeyValue]): EdgeWithIndex = {
      assert(kvs.size == 1)
      val kv = kvs.head
      /** row Key */
      val keyBytes = kv.key()
      var pos = 0
      val srcVertexId = SourceVertexId.fromBytes(keyBytes, pos, keyBytes.length, version)
      pos += srcVertexId.bytes.length
      val labelWithDir = LabelWithDirection(Bytes.toInt(keyBytes, pos, 4))
      pos += 4
      val (labelOrderSeq, isInverted) = bytesToLabelIndexSeqWithIsInverted(keyBytes, pos)

      /** qualifier */
      val qualifierBytes = kv.qualifier()
      if (qualifierBytes.isEmpty) {
        val degree = Bytes.toLong(kv.value)
        val ts = kv.timestamp
        val op = GraphUtil.operations("insert")
        val dummyProps = Map(LabelMeta.degreeSeq -> InnerVal.withLong(degree, version))
        val tgtVertexId = TargetVertexId(VertexId.DEFAULT_COL_ID, InnerVal.withStr("0", version))
        EdgeWithIndex(Vertex(srcVertexId), Vertex(tgtVertexId), labelWithDir, op,
          ts, labelOrderSeq, dummyProps)
      } else {
        pos = 0
        val op = qualifierBytes.last
        val (idxProps, tgtVertexId) = {
          val (decodedProps, endAt) = bytesToProps(qualifierBytes, pos, version)
          val decodedVId = TargetVertexId.fromBytes(qualifierBytes, endAt, qualifierBytes.length, version)
          (decodedProps, decodedVId)
        }
        val labelIndexOpt = LabelIndex.findByLabelIdAndSeq(labelWithDir.labelId, labelOrderSeq)
        assert(labelIndexOpt.isDefined)
        assert(labelIndexOpt.get.metaSeqs.length == idxProps.length)
        val idxPropsMerged = labelIndexOpt.get.metaSeqs.zip(idxProps.map(_._2))
        /** value */
        val valueBytes = kv.value()
        pos = 0
        val (props, endAt) = bytesToKeyValues(valueBytes, pos, 0, version)

        EdgeWithIndex(Vertex(srcVertexId), Vertex(tgtVertexId), labelWithDir,
          op, kv.timestamp, labelOrderSeq, (idxPropsMerged ++ props).toMap)
      }
    }

  }

  object IndexedEdgeLikeV2 extends Serializable[EdgeWithIndex, Seq[KeyValue]] {


    val version = InnerVal.VERSION2

    def encode(edgeWithIndex: EdgeWithIndex): Seq[KeyValue] = {
      val id = VertexId.toSourceVertexId(edgeWithIndex.srcVertex.id)
      /** rowKey */
      val rowKey = Bytes.add(id.bytes,
        edgeWithIndex.labelWithDir.bytes,
        labelOrderSeqWithIsInverted(edgeWithIndex.labelIndexSeq,
          edgeWithIndex.isInverted))

      /** qualifier */
      val tgtVertexIdBytes = VertexId.toTargetVertexId(edgeWithIndex.tgtVertex.id).bytes
      val idxPropsMap = edgeWithIndex.orders.toMap
      val idxPropsBytes = propsToBytes(edgeWithIndex.orders)

      // not store op byte.
      val qualifier = idxPropsMap.get(LabelMeta.toSeq) match {
        case None => Bytes.add(idxPropsBytes, tgtVertexIdBytes)
        case Some(vId) => idxPropsBytes
      }
      /** value */
      val value = propsToKeyValues(edgeWithIndex.metas.toList)

      Seq(new KeyValue(rowKey, edgeCf, qualifier, edgeWithIndex.ts, value))
    }

    def decode(kvs: Seq[KeyValue]): EdgeWithIndex = {
      assert(kvs.size == 1)
      val kv = kvs.head
      /** row Key */
      val keyBytes = kv.key()
      var pos = 0
      val srcVertexId = SourceVertexId.fromBytes(keyBytes, pos, keyBytes.length, version)
      pos += srcVertexId.bytes.length
      val labelWithDir = LabelWithDirection(Bytes.toInt(keyBytes, pos, 4))
      pos += 4
      val (labelOrderSeq, isInverted) = bytesToLabelIndexSeqWithIsInverted(keyBytes, pos)

      /** qualifier */
      val qualifierBytes = kv.qualifier()
      if (qualifierBytes.isEmpty) {
        val degree = Bytes.toLong(kv.value)
        val ts = kv.timestamp
        val op = GraphUtil.operations("insert")
        val dummyProps = Map(LabelMeta.degreeSeq -> InnerVal.withLong(degree, version))
        val tgtVertexId = TargetVertexId(VertexId.DEFAULT_COL_ID, InnerVal.withStr("0", version))
        EdgeWithIndex(Vertex(srcVertexId), Vertex(tgtVertexId), labelWithDir, op,
          ts, labelOrderSeq, dummyProps)
      } else {
        pos = 0
        val op = GraphUtil.defaultOpByte
        val (idxProps, tgtVertexId) = {
          val (decodedProps, endAt) = bytesToProps(qualifierBytes, pos, version)
          val decodedVId =
            if (endAt == qualifierBytes.length) {
              val innerValOpt = decodedProps.toMap.get(LabelMeta.toSeq)
              assert(innerValOpt.isDefined)
              TargetVertexId(VertexId.DEFAULT_COL_ID, innerValOpt.get)
            } else {
              TargetVertexId.fromBytes(qualifierBytes, endAt, qualifierBytes.length, version)
            }
          (decodedProps, decodedVId)
        }

        val labelIndexOpt = LabelIndex.findByLabelIdAndSeq(labelWithDir.labelId, labelOrderSeq)
        assert(labelIndexOpt.isDefined)
        assert(labelIndexOpt.get.metaSeqs.length == idxProps.length)
        val idxPropsMerged = labelIndexOpt.get.metaSeqs.zip(idxProps.map(_._2))

        /** value */
        val valueBytes = kv.value()
        pos = 0
        val (props, endAt) = bytesToKeyValues(valueBytes, pos, 0, version)


        EdgeWithIndex(Vertex(srcVertexId), Vertex(tgtVertexId), labelWithDir,
          op, kv.timestamp, labelOrderSeq, (idxPropsMerged ++ props).toMap)
      }

    }

  }

  object IndexedEdgeLikeV3 extends Serializable[EdgeWithIndex, Seq[KeyValue]] {

    //FIXME
    val version = InnerVal.VERSION3

    def encode(edgeWithIndex: EdgeWithIndex): Seq[KeyValue] = {
      val id = VertexId.toSourceVertexId(edgeWithIndex.srcVertex.id)
      /** rowKey */
      val rowKey = Bytes.add(id.bytes,
        edgeWithIndex.labelWithDir.bytes,
        labelOrderSeqWithIsInverted(edgeWithIndex.labelIndexSeq, false))

      val tgtVertexIdBytes = VertexId.toTargetVertexId(edgeWithIndex.tgtVertex.id).bytes
      val idxPropsMap = edgeWithIndex.orders.toMap
      val idxPropsBytes = propsToBytes(edgeWithIndex.orders)

      val qualifier = idxPropsMap.get(LabelMeta.toSeq) match {
        case None => Bytes.add(idxPropsBytes, tgtVertexIdBytes)
        case Some(vId) => idxPropsBytes
      }
      val compositeRowKey = Bytes.add(rowKey, qualifier)

      val emptyKeyValue = new KeyValue(compositeRowKey, edgeCf, Array.empty[Byte], edgeWithIndex.ts, Array.empty[Byte])

      //      assert(!edgeWithIndex.metas.isEmpty)


      val kvs = for {
        (k, v) <- edgeWithIndex.metas
      } yield {
          val q = Array[Byte](k)
//          Logger.debug(s"KV: $edgeWithIndex, $k, ${v.bytes.toList}")
          new KeyValue(compositeRowKey, edgeCf, q, edgeWithIndex.ts, v.bytes)
        }
      val rets = if (kvs.isEmpty) List(emptyKeyValue) else kvs.toList
//      Logger.error(s"$rets")
      rets
    }

    def decode(kvs: Seq[KeyValue]): EdgeWithIndex = {
      /** row Key */
      assert(!kvs.isEmpty)

      val kvHead = kvs.head
      val keyBytes = kvHead.key()
      var pos = 0
      val srcVertexId = SourceVertexId.fromBytes(keyBytes, pos, keyBytes.length, version)
      pos += srcVertexId.bytes.length
      val labelWithDir = LabelWithDirection(Bytes.toInt(keyBytes, pos, 4))
      pos += 4
      val (labelOrderSeq, isInverted) = bytesToLabelIndexSeqWithIsInverted(keyBytes, pos)
      pos += 1
//      Logger.error(s"rowKey: $srcVertexId, $labelWithDir, $isInverted")

      val (idxProps, tgtVertexId) =
        if (pos == keyBytes.length) {
          (Seq.empty[(Byte, InnerValLike)], srcVertexId)
        } else {
          val (decodedProps, endAt) = bytesToProps(keyBytes, pos, version)
          val decodedVId =
            if (endAt == keyBytes.length) {
              val innerValOpt = decodedProps.toMap.get(LabelMeta.toSeq)
              assert(innerValOpt.isDefined)
              TargetVertexId(VertexId.DEFAULT_COL_ID, innerValOpt.get)
            } else {
              TargetVertexId.fromBytes(keyBytes, endAt, keyBytes.length, version)
            }
          (decodedProps, decodedVId)
        }

//      Logger.error(s"idxProps: $idxProps, tgtVertexId: $tgtVertexId")
      val labelIndexOpt = LabelIndex.findByLabelIdAndSeq(labelWithDir.labelId, labelOrderSeq)
      assert(labelIndexOpt.isDefined)
      //      Logger.debug(s"${labelIndexOpt.get.metaSeqs}, ${idxProps}")
      //      assert(labelIndexOpt.get.metaSeqs.length == idxProps.length)
      val idxPropsMerged = labelIndexOpt.get.metaSeqs.zip(idxProps.map(_._2))

//      Logger.debug(s"$idxPropsMerged")
      val op = GraphUtil.operations("insert")
      //      val qualifierBytes = kvHead.qualifier
      //      val valueBytes = kvHead.value
//      Logger.error(s"startProps")
      val props = for {
        kv <- kvs if !(kv.qualifier.isEmpty && kv.value.isEmpty)
      } yield {
//          Logger.error(s"InnerForLoop: $kv")
          if (kv.qualifier().isEmpty && !kv.value().isEmpty) {
            LabelMeta.degreeSeq -> InnerVal.withLong(Bytes.toLong(kv.value()), version)
          } else {
            //            assert(kv.qualifier().length == 1)
            val propKey = kv.qualifier().head
            val propVal = InnerVal.fromBytes(kv.value(), 0, kv.value().length, version)
            (propKey -> propVal)
          }
        }
      val propsMap = (idxPropsMerged ++ props).toMap
//      Logger.error(s"props: $propsMap")

      val ret = propsMap.get(LabelMeta.degreeSeq) match {
        case None =>
          EdgeWithIndex(Vertex(srcVertexId), Vertex(tgtVertexId), labelWithDir, op, kvHead.timestamp(),
            labelOrderSeq, propsMap)
        case Some(degreeVal) =>
          val ts = kvHead.timestamp
          val op = GraphUtil.operations("insert")
          val dummyProps = Map(LabelMeta.degreeSeq -> degreeVal)
          val tgtVertexId = TargetVertexId(VertexId.DEFAULT_COL_ID, InnerVal.withStr("0", version))
          EdgeWithIndex(Vertex(srcVertexId), Vertex(tgtVertexId), labelWithDir, op,
            ts, labelOrderSeq, dummyProps)
      }
//      Logger.error(s"[Decoded]: $ret")
      ret
    }

    def startKey(edgeWithIndex: EdgeWithIndex): Array[Byte] = {
      val id = VertexId.toSourceVertexId(edgeWithIndex.srcVertex.id)
      /** rowKey */
      val rowKey = Bytes.add(id.bytes,
        edgeWithIndex.labelWithDir.bytes,
        labelOrderSeqWithIsInverted(edgeWithIndex.labelIndexSeq,
          edgeWithIndex.isInverted))

      val idxPropsMap = edgeWithIndex.orders.toMap
      rowKey
    }
    def stopKey(edgeWithIndex: EdgeWithIndex): Array[Byte] = {
      val id = VertexId.toSourceVertexId(edgeWithIndex.srcVertex.id)
      /** rowKey */
      val rowKey = Bytes.add(id.bytes,
        edgeWithIndex.labelWithDir.bytes,
        labelOrderSeqWithIsInverted(edgeWithIndex.labelIndexSeq, false))
      rowKey
    }
  }


  object SnapshotEdgeLikeV1 extends Serializable[EdgeWithIndexInverted, Seq[KeyValue]] {
    val version = InnerVal.VERSION1
    val isInverted = true

    def encode(edgeWithIndexInverted: EdgeWithIndexInverted): Seq[KeyValue] = {
      /** rowKey */
      val id = VertexId.toSourceVertexId(edgeWithIndexInverted.srcVertex.id)
      val rowKey = Bytes.add(id.bytes,
        edgeWithIndexInverted.labelWithDir.bytes,
        labelOrderSeqWithIsInverted(LabelIndex.defaultSeq, isInverted = isInverted))

      /** qualifier */
      val qualifier = VertexId.toTargetVertexId(edgeWithIndexInverted.tgtVertex.id).bytes
      /** value */
      val value = Bytes.add(Array.fill(1)(edgeWithIndexInverted.op),
        propsToKeyValuesWithTs(edgeWithIndexInverted.props.toSeq))

      Seq(new KeyValue(rowKey, edgeCf, qualifier, edgeWithIndexInverted.version, value))
    }

    def decode(kvs: Seq[KeyValue]): EdgeWithIndexInverted = {
      assert(kvs.size == 1)
      val kv = kvs.head
      /** row Key */
      val keyBytes = kv.key()
      var pos = 0
      val srcVertexId = SourceVertexId.fromBytes(keyBytes, pos, keyBytes.length, version)
      pos += srcVertexId.bytes.length
      val labelWithDir = LabelWithDirection(Bytes.toInt(keyBytes, pos, 4))
      pos += 4
      val (labelOrderSeq, isInverted) = bytesToLabelIndexSeqWithIsInverted(keyBytes, pos)

      /** qualifier */
      val qualifierBytes = kv.qualifier()
      pos = 0
      val tgtVertexId = TargetVertexId.fromBytes(qualifierBytes, 0, qualifierBytes.length, version)

      /** value */
      val valueBytes = kv.value()
      pos = 0
      val op = valueBytes(pos)
      pos += 1
      var (props, endAt) = bytesToKeyValuesWithTs(valueBytes, pos, version)
      EdgeWithIndexInverted(Vertex(srcVertexId), Vertex(tgtVertexId),
        labelWithDir, op, kv.timestamp(), props.toMap)
    }
  }

  object SnapshotEdgeLikeV2 extends Serializable[EdgeWithIndexInverted, Seq[KeyValue]] {
    val version = InnerVal.VERSION2
    val isInverted = true

    def encode(edgeWithIndexInverted: EdgeWithIndexInverted): Seq[KeyValue] = {
      /** rowKey */
      val id = VertexId.toSourceVertexId(edgeWithIndexInverted.srcVertex.id)
      val rowKey = Bytes.add(id.bytes,
        edgeWithIndexInverted.labelWithDir.bytes,
        labelOrderSeqWithIsInverted(LabelIndex.defaultSeq, isInverted = isInverted))

      /** qualifier */
      val qualifier = VertexId.toTargetVertexId(edgeWithIndexInverted.tgtVertex.id).bytes
      /** value */
      val value = Bytes.add(Array.fill(1)(edgeWithIndexInverted.op),
        propsToKeyValuesWithTs(edgeWithIndexInverted.props.toSeq))

      Seq(new KeyValue(rowKey, edgeCf, qualifier, edgeWithIndexInverted.version, value))
    }

    def decode(kvs: Seq[KeyValue]): EdgeWithIndexInverted = {
      assert(kvs.length == 1)
      val kv = kvs.head
      /** row Key */
      val keyBytes = kv.key()
      var pos = 0
      val srcVertexId = SourceVertexId.fromBytes(keyBytes, pos, keyBytes.length, version)
      pos += srcVertexId.bytes.length
      val labelWithDir = LabelWithDirection(Bytes.toInt(keyBytes, pos, 4))
      pos += 4
      val (labelOrderSeq, isInverted) = bytesToLabelIndexSeqWithIsInverted(keyBytes, pos)

      /** qualifier */
      val qualifierBytes = kv.qualifier()
      pos = 0
      val tgtVertexId = TargetVertexId.fromBytes(qualifierBytes, 0, qualifierBytes.length, version)

      /** value */
      val valueBytes = kv.value()
      pos = 0
      val op = valueBytes(pos)
      pos += 1
      var (props, endAt) = bytesToKeyValuesWithTs(valueBytes, pos, version)
      EdgeWithIndexInverted(Vertex(srcVertexId), Vertex(tgtVertexId),
        labelWithDir, op, kv.timestamp(), props.toMap)
    }
  }

  object SnapshotEdgeLikeV3 extends Serializable[EdgeWithIndexInverted, Seq[KeyValue]] {
    val version = InnerVal.VERSION3
    val isInverted = true

    def encode(edgeWithIndexInverted: EdgeWithIndexInverted): Seq[KeyValue] = {
      /** rowKey */
      val id = VertexId.toSourceVertexId(edgeWithIndexInverted.srcVertex.id)
      val rowKey = Bytes.add(id.bytes,
        edgeWithIndexInverted.labelWithDir.bytes,
        labelOrderSeqWithIsInverted(LabelIndex.defaultSeq, edgeWithIndexInverted.isInverted))

      val tgtVertexIdBytes = VertexId.toTargetVertexId(edgeWithIndexInverted.tgtVertex.id).bytes
      val idxPropsMap = Map.empty[Byte, InnerValLike]
      /** store 0 byte for length of indexProps. we can save this later. */
      val idxPropsBytes = propsToBytes(idxPropsMap.toSeq)

      val qualifier = idxPropsMap.get(LabelMeta.toSeq) match {
        case None => Bytes.add(idxPropsBytes, tgtVertexIdBytes)
        case Some(vId) => idxPropsBytes
      }
      val compositeRowKey = Bytes.add(rowKey, qualifier)
      val emptyKeyValue = new KeyValue(compositeRowKey, edgeCf, Array.empty[Byte], edgeWithIndexInverted.version, Array.empty[Byte])


      val kvs = for {
        (k, v) <- edgeWithIndexInverted.props
      } yield {
          val qualifier = Bytes.toBytes(k)
          val value = v.bytes
          new KeyValue(compositeRowKey, edgeCf, qualifier, v.ts, value)
        }
      emptyKeyValue :: kvs.toList
    }

    def decode(kvs: Seq[KeyValue]): EdgeWithIndexInverted = {
      /** row Key */
      assert(!kvs.isEmpty)

      val kv = kvs.head
      val keyBytes = kv.key()
      var pos = 0
      val srcVertexId = SourceVertexId.fromBytes(keyBytes, pos, keyBytes.length, version)
      pos += srcVertexId.bytes.length
      val labelWithDir = LabelWithDirection(Bytes.toInt(keyBytes, pos, 4))
      pos += 4
      val (labelOrderSeq, isInverted) = bytesToLabelIndexSeqWithIsInverted(keyBytes, pos)
      pos += 1

      val (idxProps, tgtVertexId) = {
        val (decodedProps, endAt) = bytesToProps(keyBytes, pos, version)
        val decodedVId =
          if (endAt == keyBytes.length) {
            val innerValOpt = decodedProps.toMap.get(LabelMeta.toSeq)
            assert(innerValOpt.isDefined)
            TargetVertexId(VertexId.DEFAULT_COL_ID, innerValOpt.get)
          } else {
            TargetVertexId.fromBytes(keyBytes, endAt, keyBytes.length, version)
          }
        (decodedProps, decodedVId)
      }
      val labelIndexOpt = LabelIndex.findByLabelIdAndSeq(labelWithDir.labelId, labelOrderSeq)
      assert(labelIndexOpt.isDefined)
      assert(labelIndexOpt.get.metaSeqs.length == idxProps.length)
      val idxPropsMerged = labelIndexOpt.get.metaSeqs.zip(idxProps.map(_._2))

      val op = GraphUtil.operations("insert")
      val qualifierBytes = kv.qualifier
      val valueBytes = kv.value

      /** no degree edge */
      val props = for {
        kv <- kvs if !(kv.qualifier.isEmpty && kv.value.isEmpty)
      } yield {
          assert(kv.qualifier().length == 1)
          val propKey = kv.qualifier().head
          val innerVal = InnerVal.fromBytes(kv.value(), 0, kv.value().length, version)
          val propVal = InnerValLikeWithTs(innerVal, kv.timestamp)
          (propKey -> propVal)
        }

      EdgeWithIndexInverted(Vertex(srcVertexId), Vertex(tgtVertexId), labelWithDir, op, kv.timestamp, props.toMap)
    }

    def startKey(edgeWithIndexInverted: EdgeWithIndexInverted): Array[Byte] = {
      val id = VertexId.toSourceVertexId(edgeWithIndexInverted.srcVertex.id)
      val rowKey = Bytes.add(id.bytes,
        edgeWithIndexInverted.labelWithDir.bytes,
        labelOrderSeqWithIsInverted(LabelIndex.defaultSeq, edgeWithIndexInverted.isInverted))

      val tgtVertexIdBytes = VertexId.toTargetVertexId(edgeWithIndexInverted.tgtVertex.id).bytes
      val idxPropsMap = Map.empty[Byte, InnerValLike]
      /** store 0 byte for length of indexProps. we can save this later. */
      val idxPropsBytes = propsToBytes(idxPropsMap.toSeq)

      val qualifier = idxPropsMap.get(LabelMeta.toSeq) match {
        case None => Bytes.add(idxPropsBytes, tgtVertexIdBytes)
        case Some(vId) => idxPropsBytes
      }
      val compositeRowKey = Bytes.add(rowKey, qualifier)
      compositeRowKey
    }
    def stopKey(edgeWithIndexInverted: EdgeWithIndexInverted): Array[Byte] = {
      Bytes.add(startKey(edgeWithIndexInverted), Array[Byte](0))
    }
  }


}
