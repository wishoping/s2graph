package com.daumkakao.s2graph.core

//import com.daumkakao.s2graph.core.mysqls._

import com.daumkakao.s2graph.core.models._

import com.daumkakao.s2graph.core.parsers.Where
import com.daumkakao.s2graph.core.types2._
import play.api.Logger
import scala.collection.mutable.ListBuffer
import org.apache.hadoop.hbase.util.Bytes
import GraphConstant._
import org.hbase.async.{GetRequest, ScanFilter, ColumnRangeFilter}

object Query {
  val initialScore = 1.0

  object DuplicatePolicy extends Enumeration {
    type DuplicatePolicy = Value
    val First, Sum, CountSum, Raw = Value

    def apply(policy: String): Value = {
      policy match {
        case "sum" => Query.DuplicatePolicy.Sum
        case "countSum" => Query.DuplicatePolicy.CountSum
        case "raw" => Query.DuplicatePolicy.Raw
        case _ => DuplicatePolicy.First
      }
    }
  }

}

case class Query(vertices: Seq[Vertex] = Seq.empty[Vertex], steps: List[Step] = List.empty[Step],
                 unique: Boolean = true, removeCycle: Boolean = false) {

  val labelSrcTgtInvertedMap = if (vertices.isEmpty) {
    Map.empty[Int, Boolean]
  } else {
    (for {
      step <- steps
      param <- step.queryParams
    } yield {
        param.label.id.get -> {
          param.label.srcColumn.columnName != vertices.head.serviceColumn.columnName
        }

      }).toMap
  }
}

case class Step(queryParams: List[QueryParam]) {
  lazy val excludes = queryParams.filter(qp => qp.exclude)
  lazy val includes = queryParams.filterNot(qp => qp.exclude)
  lazy val excludeIds = excludes.map(x => x.labelWithDir.labelId -> true).toMap
}

case class VertexParam(vertices: Seq[Vertex]) {
  var filters: Option[Map[Byte, InnerValLike]] = None

  def has(what: Option[Map[Byte, InnerValLike]]): VertexParam = {
    what match {
      case None => this
      case Some(w) => has(w)
    }
  }

  def has(what: Map[Byte, InnerValLike]): VertexParam = {
    this.filters = Some(what)
    this
  }

}

object RankParam {
  def apply(labelId: Int, keyAndWeights: Seq[(Byte, Double)]) = {
    new RankParam(labelId, keyAndWeights)
  }
}

class RankParam(val labelId: Int, var keySeqAndWeights: Seq[(Byte, Double)] = Seq.empty[(Byte, Double)]) {
  // empty => Count
  def defaultKey() = {
    this.keySeqAndWeights = List((LabelMeta.countSeq, 1.0))
    this
  }

  def singleKey(key: String) = {
    this.keySeqAndWeights =
      LabelMeta.findByName(labelId, key) match {
        case None => List.empty[(Byte, Double)]
        case Some(ktype) => List((ktype.seq, 1.0))
      }
    this
  }

  def multipleKey(keyAndWeights: Seq[(String, Double)]) = {
    this.keySeqAndWeights =
      for ((key, weight) <- keyAndWeights; row <- LabelMeta.findByName(labelId, key)) yield (row.seq, weight)
    this
  }
}

object QueryParam {
  lazy val empty = QueryParam(LabelWithDirection(0, 0))
}

case class QueryParam(labelWithDir: LabelWithDirection, timestamp: Long = System.currentTimeMillis()) {

  import Query.DuplicatePolicy._
  import Query.DuplicatePolicy

  val label = Label.findById(labelWithDir.labelId)
  val defaultKey = LabelIndex.defaultSeq
  val fullKey = defaultKey

  var labelOrderSeq = fullKey

  var outputField: Option[Byte] = None
  //  var start = OrderProps.empty
  //  var end = OrderProps.empty
  var limit = 10
  var offset = 0
  var rank = new RankParam(labelWithDir.labelId, List(LabelMeta.countSeq -> 1))
  var isRowKeyOnly = false
  var duration: Option[(Long, Long)] = None

  //  var direction = 0
  //  var props = OrderProps.empty
  var isInverted: Boolean = false

  //  var filters = new FilterList(FilterList.Operator.MUST_PASS_ALL)
  //  val scanFilters = ListBuffer.empty[ScanFilter]
  //  var filters = new FilterList(List.empty[ScanFilter], FilterList.Operator.MUST_PASS_ALL)
  var columnRangeFilter: ColumnRangeFilter = null
  //  var columnPaginationFilter: ColumnPaginationFilter = null
  var exclude = false
  var include = false

  var hasFilters: Map[Byte, InnerValLike] = Map.empty[Byte, InnerValLike]
  //  var propsFilters: PropsFilter = PropsFilter()
  var where: Option[Where] = None
  var duplicatePolicy = DuplicatePolicy.First
  var rpcTimeoutInMillis = 1000
  var maxAttempt = 2
  var includeDegree = false
  var tgtVertexInnerIdOpt: Option[InnerValLike] = None
  var cacheTTLInMillis: Long = -1L

  var from: Seq[(Byte, InnerValLike)] = Seq.empty
  var to: Seq[(Byte, InnerValLike)] = Seq.empty

  def isRowKeyOnly(isRowKeyOnly: Boolean): QueryParam = {
    this.isRowKeyOnly = isRowKeyOnly
    this
  }

  def isInverted(isInverted: Boolean): QueryParam = {
    this.isInverted = isInverted
    this
  }

  def labelOrderSeq(labelOrderSeq: Byte): QueryParam = {
    this.labelOrderSeq = labelOrderSeq
    this
  }

  def limit(offset: Int, limit: Int): QueryParam = {
    /** since degree info is located on first always */
    this.limit = if (offset == 0) limit + 1 else limit
    this.offset = offset
    //    this.columnPaginationFilter = new ColumnPaginationFilter(this.limit, this.offset)
    this
  }

  def interval(fromTo: Option[(Seq[(Byte, InnerValLike)], Seq[(Byte, InnerValLike)])]): QueryParam = {
    fromTo match {
      case Some((from, to)) =>
        this.from = from
        this.to = to
        interval(from, to)
      case _ => this
    }
  }

  private def startKeyAndStopKey(from: Seq[(Byte, InnerValLike)], to: Seq[(Byte, InnerValLike)]) = {
    val len = label.indicesMap(labelOrderSeq).sortKeyTypes.size.toByte
    import types2.HBaseDeserializable._
    /**
     * In natural order
     * -129, -128 , -2, -1 < 0 < 1, 2, 127, 128
     *
     * In byte order
     * 0 < 1, 2, 127, 128 < -129, -128, -2, -1
     *
     */
    val toVal = Bytes.add(propsToBytes(to), Array.fill(1)(0))
    val fromVal = Bytes.add(propsToBytes(from), Array.fill(1)(-1))
    toVal(0) = len
    fromVal(0) = len
    val maxBytes = fromVal
    val minBytes = toVal
    (minBytes, maxBytes)
  }

  def interval(from: Seq[(Byte, InnerValLike)], to: Seq[(Byte, InnerValLike)]): QueryParam = {
    val (minBytes, maxBytes) = startKeyAndStopKey(from, to)
    val rangeFilter = new ColumnRangeFilter(minBytes, true, maxBytes, true)

    //    queryLogger.info(s"Interval: ${rangeFilter.getMinColumn().toList} ~ ${rangeFilter.getMaxColumn().toList}: ${Bytes.compareTo(minBytes, maxBytes)}")
    //    this.filters.(rangeFilter)
    this.columnRangeFilter = rangeFilter
    this
  }

  def duration(minMaxTs: Option[(Long, Long)]): QueryParam = {
    minMaxTs match {
      case Some((minTs, maxTs)) => duration(minTs, maxTs)
      case _ => this
    }
  }

  def duration(minTs: Long, maxTs: Long): QueryParam = {
    this.duration = Some((minTs, maxTs))
    this
  }

  def rank(r: RankParam): QueryParam = {
    this.rank = r
    this
  }

  def exclude(filterOut: Boolean): QueryParam = {
    this.exclude = filterOut
    this
  }

  def include(filterIn: Boolean): QueryParam = {
    this.include = filterIn
    this
  }

  def outputField(ofOpt: Option[Byte]): QueryParam = {
    this.outputField = ofOpt
    this
  }

  def has(hasFilters: Map[Byte, InnerValLike]): QueryParam = {
    this.hasFilters = hasFilters
    this
  }

  def where(whereOpt: Option[Where]): QueryParam = {
    this.where = whereOpt
    this
  }

  def duplicatePolicy(policy: Option[DuplicatePolicy]): QueryParam = {
    this.duplicatePolicy = policy.getOrElse(DuplicatePolicy.First)
    this
  }

  def rpcTimeout(millis: Int): QueryParam = {
    this.rpcTimeoutInMillis = millis
    this
  }

  def maxAttempt(attempt: Int): QueryParam = {
    this.maxAttempt = attempt;
    this
  }

  def includeDegree(includeDegree: Boolean): QueryParam = {
    this.includeDegree = includeDegree
    this
  }

  def tgtVertexInnerIdOpt(other: Option[InnerValLike]): QueryParam = {
    this.tgtVertexInnerIdOpt = other
    this
  }

  def cacheTTLInMillis(other: Long): QueryParam = {
    this.cacheTTLInMillis = other
    this
  }

  override def toString(): String = {
    List(label.label, labelOrderSeq, offset, limit, rank, isRowKeyOnly,
      duration, isInverted, exclude, include, hasFilters, outputField).mkString("\t")
  }


  def buildScanRequest(srcVertex: Vertex, tgtVertexOpt: Option[Vertex] = None) = {

    val (srcColumn, tgtColumn) =
      if (labelWithDir.dir == GraphUtil.directions("in") && label.isDirected) (label.tgtColumn, label.srcColumn)
      else (label.srcColumn, label.tgtColumn)
    val (srcInnerId, tgtInnerId) =
    //FIXME
      if (labelWithDir.dir == GraphUtil.directions("in") && tgtVertexOpt.isDefined && label.isDirected) {
        // need to be swap src, tgt
        val tgtVertexInnerId = tgtVertexOpt.get.innerId
        (InnerVal.convertVersion(tgtVertexInnerId, srcColumn.columnType, label.schemaVersion),
          InnerVal.convertVersion(srcVertex.innerId, tgtColumn.columnType, label.schemaVersion))
      } else {
        val tgtVertexInnerId = tgtVertexOpt.map(t => t.innerId).getOrElse(srcVertex.innerId)
        (InnerVal.convertVersion(srcVertex.innerId, tgtColumn.columnType, label.schemaVersion),
          InnerVal.convertVersion(tgtVertexInnerId, srcColumn.columnType, label.schemaVersion))
      }
    val (srcVId, tgtVId) =
      (SourceVertexId(srcColumn.id.get, srcInnerId), TargetVertexId(tgtColumn.id.get, tgtInnerId))
    val (srcV, tgtV) = (Vertex(srcVId), Vertex(tgtVId))
    val op = GraphUtil.operations("insert")
    val ts = System.currentTimeMillis()

    val props = Map.empty[Byte, InnerValLike]
    val propsWithTs = Map.empty[Byte, InnerValLikeWithTs]
//    Logger.debug(s"[buildScanRequest]: $srcV, $tgtV, $labelWithDir, $op, $ts, $propsWithTs")
    val (startKey, stopKey) =
      if (tgtVertexOpt.isDefined) {
        val edge = EdgeWithIndexInverted(srcV, tgtV, labelWithDir, op, ts, propsWithTs)
        val keyValue = edge.keyValues.head
        (keyValue.key, Bytes.add(keyValue.key(), Array[Byte](0)))
      } else {
        val edge = EdgeWithIndex(srcV, tgtV, labelWithDir, op, ts, labelOrderSeq, props)
        val (minBytes, maxBytes) = startKeyAndStopKey(this.from, this.to)
        (Bytes.add(edge.startKey, minBytes), Bytes.add(edge.stopKey, maxBytes))
      }
    val (minTs, maxTs) = duration.getOrElse((0L, Long.MaxValue))
    val client = Graph.getClient(label.hbaseZkAddr)
    val filters = ListBuffer.empty[ScanFilter]

    val scan = client.newScanner(label.hbaseTableName)

//    Logger.debug(s"[StartKey]: ${startKey.toList}")
//    Logger.debug(s"[StopKey]: ${stopKey.toList}")
    scan.setStartKey(startKey)
    scan.setStopKey(stopKey)




    scan.setMaxVersions(1)
    scan.setFamily(edgeCf)
    scan.setMaxNumKeyValues(Byte.MaxValue)
    scan.setTimeRange(minTs, maxTs)
    scan.setMaxNumRows(limit)
    //        scan.setFilter()
    Logger.debug(s"[Scanner]: $scan")
    scan

  }

  def buildGetRequest(srcVertex: Vertex) = {
    val (srcColumn, tgtColumn) =
      if (labelWithDir.dir == GraphUtil.directions("in") && label.isDirected) (label.tgtColumn, label.srcColumn)
      else (label.srcColumn, label.tgtColumn)
    val (srcInnerId, tgtInnerId) =
    //FIXME
      if (labelWithDir.dir == GraphUtil.directions("in") && tgtVertexInnerIdOpt.isDefined && label.isDirected) {
        // need to be swap src, tgt
        val tgtVertexInnerId = tgtVertexInnerIdOpt.get
        (InnerVal.convertVersion(tgtVertexInnerId, srcColumn.columnType, label.schemaVersion),
          InnerVal.convertVersion(srcVertex.innerId, tgtColumn.columnType, label.schemaVersion))
      } else {
        val tgtVertexInnerId = tgtVertexInnerIdOpt.getOrElse(srcVertex.innerId)
        (InnerVal.convertVersion(srcVertex.innerId, tgtColumn.columnType, label.schemaVersion),
          InnerVal.convertVersion(tgtVertexInnerId, srcColumn.columnType, label.schemaVersion))
      }
    val (srcVId, tgtVId) =
      (SourceVertexId(srcColumn.id.get, srcInnerId), TargetVertexId(tgtColumn.id.get, tgtInnerId))
    val (srcV, tgtV) = (Vertex(srcVId), Vertex(tgtVId))
    val op = GraphUtil.operations("insert")
    val ts = System.currentTimeMillis()
    val props = Map.empty[Byte, InnerValLike]
    val propsWithTs = Map.empty[Byte, InnerValLikeWithTs]
    val rowKey = if (tgtVertexInnerIdOpt.isDefined) {
      EdgeWithIndexInverted(srcV, tgtV, labelWithDir, op, ts, propsWithTs).keyValues.head.key
    } else {
      EdgeWithIndex(srcV, tgtV, labelWithDir, op, ts, labelOrderSeq, props).keyValues.head.key
    }

    Logger.debug(s"getRequest: ${rowKey.toList}")
    val (minTs, maxTs) = duration.getOrElse((0L, Long.MaxValue))
    val client = Graph.getClient(label.hbaseZkAddr)
    val filters = ListBuffer.empty[ScanFilter]

    val get = new GetRequest(label.hbaseTableName.getBytes, rowKey, edgeCf)
    get.maxVersions(1)
    get.setFailfast(true)
    get.setMaxResultsPerColumnFamily(limit)
    get.setRowOffsetPerColumnFamily(offset)
    get.setMinTimestamp(minTs)
    get.setMaxTimestamp(maxTs)
    get.setMaxAttempt(maxAttempt.toByte)
    get.setRpcTimeout(rpcTimeoutInMillis)
    if (columnRangeFilter != null) get.filter(columnRangeFilter)
    Logger.debug(s"[GetRequest]: $get")
    get
  }
}
