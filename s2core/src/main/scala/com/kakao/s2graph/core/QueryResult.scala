package com.kakao.s2graph.core

import com.kakao.s2graph.core.mysqls.LabelMeta
import com.kakao.s2graph.core.storage.{SKeyValue, Storage}
import com.kakao.s2graph.core.types.{InnerVal, InnerValLikeWithTs}
import org.apache.hadoop.hbase.util.Bytes


import scala.collection.Seq

object QueryResult {

  def fromVertices(query: Query): Seq[QueryRequestWithResult] = {
    val queryParam = query.steps.head.queryParams.head
    val label = queryParam.label
    val currentTs = System.currentTimeMillis()
    val propsWithTs = Map(LabelMeta.timeStampSeq ->
      InnerValLikeWithTs(InnerVal.withLong(currentTs, label.schemaVersion), currentTs))
    for {
      vertex <- query.vertices
    } yield {
      val edge = Edge(vertex, vertex, queryParam.labelWithDir, propsWithTs = propsWithTs)
      val edgeWithScore = EdgeWithScore(edge, Graph.DefaultScore)
      QueryRequestWithResult(QueryRequest(query, -1, vertex, queryParam),
        QueryResult(edgeWithScoreLs = Seq(edgeWithScore)))
    }
  }

  def toBytes(storage: Storage)(queryResultLs: Seq[QueryResult]): Array[Byte] = {
    val len = queryResultLs.size
    var bytes = Bytes.toBytes(len)
    queryResultLs.foreach { queryResult =>
      bytes = Bytes.add(bytes, toBytesSingle(storage)(queryResult))
    }
    bytes
  }

  def toBytesSingle(storage: Storage)(queryResult: QueryResult): Array[Byte] = {
    val len = queryResult.edgeWithScoreLs.size
    var bytes = Bytes.toBytes(len)
    queryResult.edgeWithScoreLs.foreach { edgeWithScore =>
      val kv = storage.snapshotEdgeSerializer(edgeWithScore.edge.toSnapshotEdge).toKeyValues.head
      bytes = Bytes.add(bytes, kv.bytes, Bytes.toBytes(edgeWithScore.score))
    }
    bytes = Bytes.add(bytes, Bytes.toBytes(queryResult.timestamp))
    bytes
  }

  def fromBytes(storage: Storage, queryRequest: QueryRequest)(bytes: Array[Byte], offset: Int): Seq[QueryResult] = {
    val len = Bytes.toInt(bytes, offset, 4)
    var pos = offset + 4
    for {
      i <- (0 until len)
    } yield {
      val (queryResult, endAt) = fromBytesSingle(storage, queryRequest)(bytes, pos)
      pos = endAt
      queryResult
    }
  }

  def fromBytesSingle(storage: Storage, queryRequest: QueryRequest)(bytes: Array[Byte], offset: Int): (QueryResult, Int) = {
    val len = Bytes.toInt(bytes, offset, 4)
    var pos = offset + 4
    val edgeWithScores = for {
      i <- (0 until len)
    } yield {
        val (kv, endAt) = SKeyValue.fromBytes(bytes, pos)
        pos = endAt + 8
        // this can be optimized with cache.
        val (query, stepIdx, vertex, queryParam) = QueryRequest.unapply(queryRequest).get
        val edge = storage.snapshotEdgeDeserializer.fromKeyValues(queryParam, Seq(kv), queryParam.label.schemaVersion, None)
        val score = Bytes.toDouble(bytes, endAt)
        EdgeWithScore(edge.toEdge, score)
      }
    val timestamp = Bytes.toLong(bytes, pos, 8)
    pos += 8
    val endAt = pos
    (QueryResult(edgeWithScores, timestamp), endAt)
  }

}
case class QueryRequestWithResult(queryRequest: QueryRequest, queryResult: QueryResult)

case class QueryRequest(query: Query,
                        stepIdx: Int,
                        vertex: Vertex,
                        queryParam: QueryParam)


case class QueryResult(edgeWithScoreLs: Seq[EdgeWithScore] = Nil,
                       timestamp: Long = System.currentTimeMillis(),
                       isFailure: Boolean = false)

case class EdgeWithScore(edge: Edge, score: Double)
