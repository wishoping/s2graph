package com.kakao.s2graph.core.utils

import scala.concurrent.Future

object FutureOps {

  def retry[T](n: Int)(f: => Future[T]): Future[T] = {
    n match {
      case i if i > 1 => f recoverWith { case t: Throwable => retry(n - 1)(f) }
      case _ =>
        logger.error(s"Future wait failed")
        f
    }
  }

}