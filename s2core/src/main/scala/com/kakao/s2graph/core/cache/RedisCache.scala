package com.kakao.s2graph.core.cache


import java.util.concurrent.TimeUnit

import com.google.common.cache.CacheBuilder
import com.kakao.s2graph.core.storage.hbase.{AsynchbaseStorage, AsynchbaseQueryBuilder}
import com.kakao.s2graph.core.{QueryRequest, QueryResult}
import com.kakao.s2graph.core.storage.{SCache}
import com.typesafe.config.Config
import org.hbase.async.GetRequest
import scala.concurrent.{Promise, Future, ExecutionContext}

class RedisCache(config: Config, storage: AsynchbaseStorage)(implicit ec: ExecutionContext)
  extends SCache[QueryRequest, Future[Seq[QueryResult]]] {

  val clients = new WithRedis(config)

  val builder = new AsynchbaseQueryBuilder(storage)

  val maxSize = 10000
  val cache = CacheBuilder.newBuilder().expireAfterWrite(100, TimeUnit.MILLISECONDS)
  .maximumSize(maxSize).build[java.lang.Long, Future[Seq[QueryResult]]]()

  private def buildRequest(queryRequest: QueryRequest): GetRequest = builder.buildRequest(queryRequest)
  private def toCacheKeyBytes(getRequest: GetRequest): Array[Byte] = builder.toCacheKeyBytes(getRequest)

  private def toCacheKey(queryRequest: QueryRequest): Long =
    queryRequest.queryParam.toCacheKey(toCacheKeyBytes(buildRequest(queryRequest)))

  private def getBytes(value: Any): Array[Byte] = value.toString().getBytes("UTF-8")
  private def toTs(queryRequest: QueryRequest): Int = (queryRequest.queryParam.cacheTTLInMillis / 1000).toInt

  override def getIfPresent(queryRequest: QueryRequest): Future[Seq[QueryResult]] = {
    val key = toCacheKey(queryRequest)
    val promise = Promise[Seq[QueryResult]]
    cache.asMap().putIfAbsent(key, promise.future) match {
      case null =>
        val future = Future {
          clients.doBlockWithKey(key.toString) { jedis =>
            val v = jedis.get(getBytes(key))
            if (v == null) Nil
            else {
              QueryResult.fromBytes(storage, queryRequest)(v, 0)
            }
          }
        }
        future.onComplete { value =>
          promise.complete(value)
          if (value.isFailure) cache.asMap().remove(key, promise.future)
        }
        future
      case existingFuture => existingFuture
    }

  }

  override def put(queryRequest: QueryRequest, queryResultLsFuture: Future[Seq[QueryResult]]): Unit = {
    queryResultLsFuture.onComplete { queryResultLsTry =>
      if (queryResultLsTry.isSuccess) {
        val key = toCacheKey(queryRequest)
        val queryResultLs = queryResultLsTry.get
        val bytes = QueryResult.toBytes(storage)(queryResultLs)
        clients.doBlockWithKey(key.toString) { jedis =>
          jedis.setex(getBytes(key), toTs(queryRequest), bytes)
        }
      }
    }
  }
}