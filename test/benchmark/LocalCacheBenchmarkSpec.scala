package benchmark

import com.kakao.s2graph.core.mysqls.{LabelMeta, Label}
import com.kakao.s2graph.core._
import com.kakao.s2graph.core.storage.hbase.{AsynchbaseStorage, AsynchbaseQueryBuilder}
import com.kakao.s2graph.core.types.{InnerValLikeWithTs, LabelWithDirection, InnerVal, VertexId}
import com.typesafe.config.ConfigFactory
import controllers.SpecCommon
import play.api.test.{FakeApplication, FakeRequest, PlaySpecification}

import scala.collection.mutable

class LocalCacheBenchmarkSpec extends SpecCommon with BenchmarkCommon with PlaySpecification {


  "LocalCacheBenchmarkSpec" should {
    "memory usages for queryResults" in {
      running(FakeApplication()) {
        val label = Label.findByName(testLabelName).get
        val vertexId = VertexId(1, InnerVal.withLong(10, label.schemaVersion))
        val vertex = Vertex(vertexId)
        val labelWithDir = LabelWithDirection(label.id.get, 0)
        val ts = System.currentTimeMillis()

        val maxSize = 10000
        val numOfKey = maxSize * 10
        val numOfVal = 100
        val numOfProps = 2

        def extraPropsWithTs = (1 to numOfProps).map { ith => ith.toByte -> InnerValLikeWithTs.withLong(ts, ts, label.schemaVersion)} toMap
        def propsWithTs = Map(LabelMeta.timeStampSeq -> InnerValLikeWithTs.withLong(ts, ts, label.schemaVersion)) ++ extraPropsWithTs

        def newEdge = Edge(vertex, vertex, labelWithDir, propsWithTs = propsWithTs)
        val queryParam = QueryParam(labelWithDir)
        val query = Query.toQuery(Seq(vertex), queryParam)



        var avg = 0.0
        var cnt = 0
        val config = ConfigFactory.parseString(s"cache.max.size=${maxSize}").withFallback(Graph.DefaultConfig)
        val graph = new Graph(config)
        val cache = graph.storage.cacheOpt.get


        val runtime = Runtime.getRuntime()
        Thread.sleep(1000)
        println("starting benchmark")
        val startMemory = runtime.totalMemory() - runtime.freeMemory()


        for {
          i <- (0 until numOfKey)
        } {
          if (i % 1000 == 0) {
            val endMemory = runtime.totalMemory() - runtime.freeMemory()
            val keySize = if (i > maxSize) maxSize else i
            val used = (endMemory - startMemory) / 1024 / 1024
            println(s"Usaged memory for key[$keySize], value[$numOfVal]: ${used} MB")
          }
          val edgesWithScore = (0 until numOfVal).map(_ => EdgeWithScore(newEdge, 1.0))
          val queryResult = QueryResult(query, 0, queryParam, edgesWithScore)
          avg += queryResult.edgeWithScoreLs.size
          cnt += 1
          cache.put(i.toLong, Seq(queryResult))
        }
        println(s"${cache.size()} : ${avg / cnt}")
        val endMemory = runtime.totalMemory() - runtime.freeMemory()
        Thread.sleep(1000)
        println(s"Usaged memory for key[$numOfKey], value[$numOfVal]: ${endMemory - startMemory}")
        true
      }
    }

    "collisions per queryRequests" in {
      running(FakeApplication()) {
        val label = Label.findByName(testLabelName).get
        val labelWithDir = LabelWithDirection(label.id.get, 0)
        val queryParam = QueryParam(labelWithDir)
        val ts = System.currentTimeMillis()
        val query = Query.toQuery(Nil, queryParam)
        def getQueryRequest(query: Query, queryParam: QueryParam, srcId: Long) = {
          val vertex = Vertex(VertexId(1, InnerVal.withLong(srcId, label.schemaVersion)))
          QueryRequest(query, 0, vertex, queryParam, 1.0)
        }

        val queryResult = QueryResult(query, 0, queryParam)

        val maxSize = 100000
        val numOfKey = 100000000
        val stepSize = numOfKey / 100
        var collision = 0
        val config = ConfigFactory.parseString(s"cache.max.size=${maxSize}").withFallback(Graph.DefaultConfig)
        val graph = new Graph(config)
        val cache = graph.storage.cacheOpt.get
        val queryBuilder = new AsynchbaseQueryBuilder(graph.storage.asInstanceOf[AsynchbaseStorage])

        val set = new mutable.HashSet[Long]()
        println(Console.GREEN +"This takes long so go get a cup of coffee.")
        for {
          i <- (0 until numOfKey)
        } {
          if (i % stepSize == 0) {
            println(Console.GREEN +
              s">>${i * 100L / numOfKey}% processed: key[$i], collision[$collision]")
          }
          val queryRequest = getQueryRequest(query, queryParam, i)
          val request = queryBuilder.buildRequest(queryRequest)
          val cacheKey = queryParam.toCacheKey(queryBuilder.toCacheKeyBytes(request))
          if (set.contains(cacheKey)) collision += 1
          else set += cacheKey
        }
        println(Console.GREEN + "=" * 50)
        println(Console.GREEN + s"Collision for ${numOfKey}: ${collision}")
        println(Console.GREEN + "=" * 50)
        true
      }
    }
  }

}
