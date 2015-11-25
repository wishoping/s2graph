package com.kakao.s2graph.core.storage

trait SCache[K, V] {
  def getIfPresent(key: K): V
  def put(key: K, value: V): Unit
}
