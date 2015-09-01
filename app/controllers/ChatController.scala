package controllers

import com.daumkakao.s2graph.core.{Graph, Management, GraphUtil}
import play.api.libs.json.{JsValue, Json}
import play.api.mvc.{Action, Controller}

import scala.concurrent.Future
import scala.util.Try

/**
 * Created by shon on 9/1/15.
 */
object ChatController extends Controller with RequestParser {

  import scala.concurrent.ExecutionContext.Implicits.global

  private val userChatIdLabel = "user_id_chat_ids"
  private val chatIdMessageIdsLabel = "chat_id_message_ids"
  private val chatNameIdsLabel = "chat_name_ids" // timestamp is last updated at when message is sent to this chat
//  private val userLastVisitedAtLabel = "user_id_last_visited_at"

  private val delimiter = ","
  /** Admin */
  val userChatIdLabelCreate =
    s"""
       |
     """.stripMargin
  def createChatService() = {


  }

  /** OLTP */

  private def toChatName(serviceName: String, chatName: String): String = {
    serviceName + ":" + chatName
  }

  private def toChatId(serviceName: String, chatName: String): Long = {
    GraphUtil.murmur3(toChatName(serviceName, chatName))
  }

  private def toMsgId(chatId: String, msg: String): Long = {
    GraphUtil.murmur3(chatId + ":" + msg)
  }

  /** write */
  def createChat(serviceName: String, userId: String, chatName: String) = Action.async { request =>
    createChatInner(serviceName)(userId, chatName).map { chatId =>
      Ok(chatId.toString())
    }
  }
  def createChatInner(serviceName: String)(userId: String, chatName: String): Future[Long] = {
    val ts = System.currentTimeMillis()
    val chatNameInner = toChatName(serviceName, chatName)
    val chatId = toChatId(serviceName, chatName)

    val userChatIdEdge = toEdge(Json.obj("timestamp" -> ts, "from" -> userId, "to" -> chatId,
      "label" -> userChatIdLabel,
      "props" -> Json.obj("isAuthor" -> true)), "insert")

    val chatNameIdsEdge = toEdge(Json.obj("timestamp" -> ts, "from" -> chatNameInner, "to" -> chatId,
      "label" -> chatNameIdsLabel,
      "props" -> Json.obj("creatorId" -> userId,
        "serviceName" -> serviceName, "chatName" -> chatName)), "insert")

    Graph.mutateEdges(Seq(userChatIdEdge, chatNameIdsEdge)).map { rets =>
      if (rets.forall(identity)) chatId
      else throw new RuntimeException("createChatRoom failed.")
    }
  }

  def joinChat(serviceName: String, chatId: String, userIds: String) = Action.async { request =>
    joinChatInner(serviceName)(chatId, userIds.split(delimiter)).map { rets =>
      Ok(rets.toString)
    }
  }
  def joinChatInner(serviceName: String)(chatId: String, userIds: Seq[String]): Future[Seq[Boolean]] = {
    val ts = System.currentTimeMillis()
    val jsValue = Json.toJson(userIds.map { userId =>
      Json.obj("timestamp" -> ts, "from" -> userId, "to" -> chatId, "label" -> userChatIdLabel,
        "props" -> Json.obj())
    })
    val edges = toEdges(jsValue, "insert")
    Graph.mutateEdges(edges)
  }

  def sendMsgToChat(serviceName: String, userId: String, chatName: String, chatId: String) = Action.async(parse.json) { request =>
    val messages = request.body.asOpt[List[String]].getOrElse(Nil)
    sendMsgToChatInner(serviceName)(userId, chatName, chatId, messages).map { rets =>
      Ok(rets.toString)
    }
  }
  def sendMsgToChatInner(serviceName: String)(userId: String, chatName: String, chatId: String, messages: Seq[String]): Future[Seq[Boolean]] = {
    val ts = System.currentTimeMillis()
    val jsValue = Json.toJson(messages.map { msg =>

      Json.obj("timestamp" -> ts, "from" -> chatId, "to" -> toMsgId(chatId, msg), "label" -> chatIdMessageIdsLabel,
        "props" -> Json.obj("senderId" -> userId, "message" -> msg))
    })
    val chatNameIdEdge = toEdge(Json.obj("timestamp" -> ts, "from" -> chatName, "to" -> chatId, "label" -> chatNameIdsLabel), "update")

    val edges = toEdges(jsValue, "insert") :+ chatNameIdEdge
    Graph.mutateEdges(edges)
  }
  def updateUserChat(serviceName: String, userId: String, chatId: String) = Action.async { request =>
    updateUserChatInner(serviceName)(userId, chatId).map { ret =>
      Ok(ret.toString)
    }
  }
  def updateUserChatInner(serviceName: String)(userId: String, chatId: String): Future[Boolean] = {
    val ts = System.currentTimeMillis()
    val jsValue = Json.obj("timestamp" -> ts, "from" -> userId, "to" -> chatId, "label" -> userChatIdLabel)
    val edge = toEdge(jsValue, "update")
    Graph.mutateEdge(edge)
  }
//  def updateUserLastVisitedAt(serviceName: String)(userId: String, chatId: String) = Action.async { request =>
//    updateUserLastVisitedAtInner(serviceName)(userId, chatId).map { ret =>
//      Ok(ret.toString)
//    }
//  }
//
//  def updateUserLastVisitedAtInner(serviceName: String)(userId: String, chatId: String): Future[Boolean] = {
//    val ts = System.currentTimeMillis()
//    val jsValue = Json.obj("timestamp" -> ts, "from" -> userId, "to" -> userId, "label" -> userLastVisitedAtLabel,
//    "props" -> Json.obj())
//    val edge = toEdge(jsValue, "update")
//    Graph.mutateEdge(edge)
//  }

  def deleteMsgInChatId(serviceName: String, chatId: String, msgId: String) = Action.async { request =>
    deleteMsgInChatIdInner(serviceName, chatId, msgId).map { ret =>
      Ok(ret.toString())
    }
  }
  def deleteMsgInChatIdInner(serviceName: String, chatId: String, msgId: String): Future[Boolean] = {
    val ts = System.currentTimeMillis()
    val jsValue = Json.obj("timestamp" -> ts, "from" -> chatId, "to" -> msgId, "label" -> chatIdMessageIdsLabel, "props" -> Json.obj())
    val edge = toEdge(jsValue, "delete")
    Graph.mutateEdge(edge)
  }

  /** read */
  def usersInChatId(serviceName: String, chatId: String) = Action.async { request =>
    val columnName = "chat_id"
    val jsValue =
      Json.obj(
        "srcVertices" -> Json.arr(Json.obj("id" -> chatId, "serviceName" -> serviceName, "columnName" -> columnName)),
        "steps" -> Json.arr(
          Json.obj(
            "step" -> Json.arr(
              Json.obj("label" -> userChatIdLabel, "limit" -> 100, "direction" -> "in")
            )
          )
        )
      )
    QueryController.getEdgesInner(jsValue)
  }
  def userChatIds(serviceName: String, userId: String) = Action.async { request =>
    val columnName = "user_id"
    val jsValue =
      Json.obj(
        "srcVertices" -> Json.arr(Json.obj("id" -> userId, "serviceName" -> serviceName, "columnName" -> columnName)),
        "steps" -> Json.arr(
          Json.obj(
            "step" -> Json.arr(
              Json.obj("label" -> userChatIdLabel, "limit" -> 100)
            )
          )
        )
      )
    QueryController.getEdgesInner(jsValue)
  }

  def userChatIdsWithName(serviceName: String, userId: String) = Action.async { request =>
    val columnName = "user_id"
    val jsValue =
      Json.obj(
        "srcVertices" -> Json.arr(Json.obj("id" -> userId, "serviceName" -> serviceName, "columnName" -> columnName)),
        "steps" -> Json.arr(
          Json.obj(
            "step" -> Json.arr(
              Json.obj("label" -> userChatIdLabel, "limit" -> 100)
            )
          ),
          Json.obj(
            "step" -> Json.arr(
              Json.obj("label" -> chatNameIdsLabel, "direction" -> "in", "limit" -> 1)
            )
          )
        )
      )
    QueryController.getEdgesInner(jsValue)
  }

  def chatIdMessages(serviceName: String, chatId: String, lastTs: Long) = Action.async { request =>
    val columnName = "chat_id"
    val jsValue =
      Json.obj(
        "srcVertices" -> Json.arr(Json.obj("id" -> chatId, "serviceName" -> serviceName, "columnName" -> columnName)),
        "steps" -> Json.arr(
          Json.obj(
            "step" -> Json.arr(Json.obj("label" -> chatIdMessageIdsLabel, "limit" -> 100, "duration" ->
            Json.obj("from" -> lastTs, "to" -> System.currentTimeMillis())))
          )
        )
      )
    QueryController.getEdgesInner(jsValue)
  }

  def chatIdsMessages(serviceName: String, userId: String, lastTs: Long) = Action.async { request =>
    val columnName = "user_id"
    val jsValue =
      Json.obj(
        "srcVertices" -> Json.arr(Json.obj("id" -> userId, "serviceName" -> serviceName, "columnName" -> columnName)),
        "steps" -> Json.arr(
          Json.obj(
            "step" -> Json.arr(Json.obj("label" -> userChatIdLabel, "limit" -> 100))
          ),
          Json.obj(
            "step" -> Json.arr(Json.obj("label" -> chatIdMessageIdsLabel, "limit" -> 100, "duration" ->
            Json.obj("from" -> lastTs, "to" -> System.currentTimeMillis())))
          )
        )
      )
    QueryController.getEdgesInner(jsValue)
  }

}
