package com.kakao.s2graph.core.utils

import java.util.concurrent.atomic.AtomicBoolean

import com.google.common.cache.CacheBuilder
import play.api.Logger
import play.api.libs.json.JsValue

import scala.concurrent.{Future, ExecutionContext}
import scala.util.{Success, Failure}

/**
 * Created by daewon on 2015. 11. 4..
 */

object SafeUpdateCache {

  case class CacheKey(key: String)

}

class SafeUpdateCache[T](prefix: String, maxSize: Int, ttl: Int)(implicit executionContext: ExecutionContext) {

  import SafeUpdateCache._

  implicit class StringOps(key: String) {
    def toCacheKey = new CacheKey(prefix + ":" + key)
  }

  def toTs() = (System.currentTimeMillis() / 1000).toInt

  private val cache = CacheBuilder.newBuilder().maximumSize(maxSize).build[CacheKey, (T, Int, AtomicBoolean)]()

  def put(key: String, value: T) = cache.put(key.toCacheKey, (value, toTs, new AtomicBoolean(false)))

  def invalidate(key: String) = cache.invalidate(key.toCacheKey)

  def withCache(key: String)(op: => T): T = {
    val newKey = key.toCacheKey
    val cachedValWithTs = cache.getIfPresent(newKey)

    if (cachedValWithTs == null) {
      // fetch and update cache.
      val newValue = op
      cache.put(newKey, (newValue, toTs(), new AtomicBoolean(false)))
      newValue
    } else {
      val (cachedVal, updatedAt, isUpdating) = cachedValWithTs
      if (toTs() < updatedAt + ttl) cachedVal
      else {
        val running = isUpdating.getAndSet(true)
        if (running) cachedVal
        else {
          Future {
            val newValue = op
            val newUpdatedAt = toTs()
            cache.put(newKey, (newValue, newUpdatedAt, new AtomicBoolean(false)))
            newValue
          }(executionContext) onComplete {
            case Failure(ex) => logger.error(s"withCache update failed.")
            case Success(s) => logger.info(s"withCache update success: $newKey")
          }
          cachedVal
        }
      }
    }
  }
}

