package controllers

import com.daumkakao.s2graph.core.Graph
import com.stumbleupon.async.Callback
import play.api.Logger
import util.TestDataLoader
import play.api.mvc.{ Action, Controller, Result }
import play.api.libs.json.Json

import scala.concurrent.Promise
import scala.collection.JavaConversions._

object TestController extends Controller  {

//  def getRandomId(friendCount: Int) = Action { request =>
//    val idOpt = TestDataLoader.randomId(friendCount)
//    val id = idOpt.getOrElse(-1)
//    Ok(s"${id}")
//  }
  import ApplicationController._
  import play.api.libs.concurrent.Execution.Implicits.defaultContext
  def getRandomId() = withHeader { request =>
    val id = TestDataLoader.randomId
    Ok(s"${id}")
  }

  def scannerTest() = Action.async { request =>
    val rowKey = Array[Byte](18, 99, -26, 114, -3, 0, 0, 1, 104, 2, 4, -22, -22, -1, -1, -26, 114, -3)
    val client = Graph.getClient("localhost")
    val scanner = client.newScanner("s2graph-dev")
    scanner.setStartKey(rowKey)
    Logger.debug(s"[Scanner]: $scanner")
    val promise = Promise
    val future = Graph.deferredToFutureWithoutFallback(scanner.nextRows())
    future.map { rows =>
//      scanner.close()
      for {
        row <- rows
        kv <- row
      } {
        Logger.debug(s"$kv")
      }
      Ok("\n")
    }
  }
}