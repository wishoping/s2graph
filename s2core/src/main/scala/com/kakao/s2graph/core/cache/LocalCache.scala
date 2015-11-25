package com.kakao.s2graph.core.cache

import com.google.common.cache.CacheBuilder
import com.kakao.s2graph.core.QueryResult
import com.kakao.s2graph.core.storage.SCache
import com.typesafe.config.Config

import scala.collection.Seq

class LocalCache(config: Config) extends SCache[java.lang.Long, Seq[QueryResult]]{
  val cacheSize = config.getInt("cache.max.size")
  val cache =
    CacheBuilder.newBuilder().
    maximumSize(cacheSize).build[java.lang.Long, Seq[QueryResult]]()

  override def getIfPresent(key: java.lang.Long): Seq[QueryResult] = {
    val cacheVal = cache.getIfPresent(key)
    if (cacheVal == null) Seq.empty
    else cacheVal
  }

  override def put(key: java.lang.Long, value: Seq[QueryResult]): Unit = {
    cache.put(key, value)
  }
}
