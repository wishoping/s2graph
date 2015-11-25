package benchmark

import com.kakao.s2graph.core._
import com.kakao.s2graph.core.mysqls.Label
import com.kakao.s2graph.core.storage.SKeyValue
import com.kakao.s2graph.core.types.{InnerVal, VertexId, LabelWithDirection}
import controllers.SpecCommon
import play.api.test.{FakeApplication, PlaySpecification}

class QueryResultSpec extends BenchmarkCommon with PlaySpecification with SpecCommon {
  "QueryResult" should {
    "serialize/deserialize SKeyValue" in {
      val table = Array[Byte](1, 4, 2)
      val row = Array[Byte](1, 2)
      val cf = Array[Byte](1, 3)
      val qualifier = Array[Byte](-1, 2)
      val value = Array[Byte](-12, 30)
      val timestamp = 10L
      val sKeyValue = SKeyValue(table, row, cf, qualifier, value, timestamp)
      val (back, endAt) = SKeyValue.fromBytes(sKeyValue.bytes, 0)
      sKeyValue.table must beEqualTo(back.table)
      sKeyValue.row must beEqualTo(back.row)
      sKeyValue.cf must beEqualTo(back.cf)
      sKeyValue.qualifier must beEqualTo(back.qualifier)
      sKeyValue.value must beEqualTo(back.value)
      true
    }
    "serialize/deserialize" in {
      running(FakeApplication()) {
        val label = Label.findByName(testLabelName).getOrElse(throw new Exception("!!"))
        val labelWithDir = LabelWithDirection(label.id.get, 0)
        val queryParam = QueryParam(labelWithDir)
        val graph = new Graph(Graph.DefaultConfig)

        val id = 1L
        val vertex = Vertex(VertexId(label.srcColumn.id.get, InnerVal.withLong(id, label.schemaVersion)))
        val query = Query.toQuery(Seq(vertex), queryParam)
        val queryRequestWithResultLs = QueryResult.fromVertices(query)
        val head = queryRequestWithResultLs.head
        val bytes = QueryResult.toBytes(graph.storage)(Seq(head.queryResult))
        val decodedQueryResultLs = QueryResult.fromBytes(graph.storage, head.queryRequest)(bytes, 0)

        for {
          (queryRequestWithResult, decodedResult) <- queryRequestWithResultLs.zip(decodedQueryResultLs)
          (queryRequest, queryResult) = QueryRequestWithResult.unapply(queryRequestWithResult).get
          (edgeWithScore, back) <- queryResult.edgeWithScoreLs.zip(decodedResult.edgeWithScoreLs)
        } {
          println(edgeWithScore + "\n" + back + "\n\n")
          edgeWithScore must beEqualTo(back)
        }
        true
      }
    }
  }
}
