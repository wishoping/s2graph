package com.kakao.s2graph.core.cache

import com.kakao.s2graph.core.storage.hbase.{AsynchbaseStorage, AsynchbaseQueryBuilder}
import com.kakao.s2graph.core.{QueryRequest, QueryResult}
import com.kakao.s2graph.core.storage.{SCache}
import com.typesafe.config.Config
import org.hbase.async.GetRequest
import scala.concurrent.ExecutionContext

class RedisCache(config: Config, storage: AsynchbaseStorage)(implicit ec: ExecutionContext) extends SCache[QueryRequest, Seq[QueryResult]] {

  val clients = new WithRedis(config)

  val builder = new AsynchbaseQueryBuilder(storage)

  private def buildRequest(queryRequest: QueryRequest): GetRequest = builder.buildRequest(queryRequest)
  private def toCacheKeyBytes(getRequest: GetRequest): Array[Byte] = builder.toCacheKeyBytes(getRequest)

  private def toCacheKey(queryRequest: QueryRequest): Long =
    queryRequest.queryParam.toCacheKey(toCacheKeyBytes(buildRequest(queryRequest)))

  private def getBytes(value: Any): Array[Byte] = value.toString().getBytes("UTF-8")
  private def toTs(queryRequest: QueryRequest): Int = (queryRequest.queryParam.cacheTTLInMillis / 1000).toInt

  override def getIfPresent(queryRequest: QueryRequest): Seq[QueryResult] = {
    val key = toCacheKey(queryRequest)
    clients.doBlockWithKey(key.toString) { jedis =>
      val v = jedis.get(getBytes(key))
      if (v == null) Nil
      else {
        QueryResult.fromBytes(storage, queryRequest)(v, 0)
      }
    }
  }

  override def put(queryRequest: QueryRequest, queryResultLs: Seq[QueryResult]): Unit = {
    val key = toCacheKey(queryRequest)
    val bytes = QueryResult.toBytes(storage)(queryResultLs)
    clients.doBlockWithKey(key.toString) { jedis =>
      jedis.setex(getBytes(key), toTs(queryRequest), bytes)
    }
  }
}