package benchmark

import com.kakao.s2graph.core.mysqls.{LabelMeta, Label}
import com.kakao.s2graph.core._
import com.kakao.s2graph.core.types.{InnerValLikeWithTs, LabelWithDirection, InnerVal, VertexId}
import com.typesafe.config.ConfigFactory
import config.Config
import controllers.SpecCommon
import play.api.test.PlaySpecification
import play.api.libs.json._
import play.api.test.{FakeApplication, FakeRequest, PlaySpecification}
import play.api.{Application => PlayApplication}
/**
 * Created by shon on 11/19/15.
 */
class LocalCacheBenchmarkSpec extends SpecCommon with BenchmarkCommon with PlaySpecification {

  "LocalCacheBenchmarkSpec" should {
    "maxSize for edges" in {
      running(FakeApplication()) {
        val label = Label.findByName(testLabelName).get
        val vertexId = VertexId(1, InnerVal.withLong(10, label.schemaVersion))
        val vertex = Vertex(vertexId)
        val labelWithDir = LabelWithDirection(label.id.get, 0)
        val ts = System.currentTimeMillis()

        val maxSize = 1000000
        val numOfKey = maxSize * 10
        val numOfVal = 1000
        val numOfProps = 10

        val extraPropsWithTs = (1 to numOfProps).map { ith => ith.toByte -> InnerValLikeWithTs.withLong(ts, ts, label.schemaVersion)} toMap
        val propsWithTs = Map(LabelMeta.timeStampSeq -> InnerValLikeWithTs.withLong(ts, ts, label.schemaVersion)) ++ extraPropsWithTs

        val edge = Edge(vertex, vertex, labelWithDir, propsWithTs = propsWithTs)
        val queryParam = QueryParam(labelWithDir)
        val query = Query.toQuery(Seq(vertex), queryParam)



        var avg = 0.0
        var cnt = 0
        val config = ConfigFactory.parseString(s"cache.max.size=${maxSize}").withFallback(Graph.DefaultConfig)
        val graph = new Graph(config)
        val cache = graph.storage.cacheOpt.get


        val runtime = Runtime.getRuntime()
        val startMemory = runtime.totalMemory() - runtime.freeMemory()


        val edgeWithScores = (0 until numOfVal).map(_ => EdgeWithScore(edge, 1.0))
        val queryResult = QueryResult(query, 0, queryParam, edgeWithScores)



        for {
          i <- (0 until numOfKey)
        } {
          avg += queryResult.edgeWithScoreLs.size
          cnt += 1
          cache.put(i, Seq(queryResult))
        }
        println(s"${cache.size()} : ${avg / cnt}")
        val endMemory = runtime.totalMemory() - runtime.freeMemory()
        println(s"Usaged memory for key[$numOfKey], value[$numOfVal]: ${endMemory - startMemory}")
        true
      }
    }
  }

}