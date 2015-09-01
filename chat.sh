curl -XPOST localhost:9000/graphs/createService -H 'Content-Type: Application/json' -d '
{
    "serviceName": "s2chat", "compressionAlgorithm": "gz"
}
'
curl -XPOST localhost:9000/graphs/createLabel -H 'Content-Type: Application/json' -d '
{
    "label": "user_id_chat_ids",
    "srcServiceName": "s2chat",
    "srcColumnName": "user_id",
    "srcColumnType": "string",
    "tgtServiceName": "s2chat",
    "tgtColumnName": "chat_id",
    "tgtColumnType": "string",
    "isDirected" : "true",
    "indices": [],
    "props": [
        {"name": "isAuthor", "defaultValue": false, "dataType": "boolean"}
    ],
    "consistencyLevel": "strong"
}'


curl -XPOST localhost:9000/graphs/createLabel -H 'Content-Type: Application/json' -d '
{
    "label": "chat_id_message_ids",
    "srcServiceName": "s2chat",
    "srcColumnName": "chat_id",
    "srcColumnType": "string",
    "tgtServiceName": "s2chat",
    "tgtColumnName": "message_id",
    "tgtColumnType": "string",
    "isDirected" : "true",
    "indices": [],
    "props": [
        {"name": "senderId", "defaultValue": "", "dataType": "string"},
        {"name": "message", "defaultValue": "", "dataType": "string"}
    ],
    "consistencyLevel": "strong"
}'

curl -XPOST localhost:9000/graphs/createLabel -H 'Content-Type: Application/json' -d '
{
    "label": "chat_name_ids",
    "srcServiceName": "s2chat",
    "srcColumnName": "chat_name",
    "srcColumnType": "string",
    "tgtServiceName": "s2chat",
    "tgtColumnName": "chat_id",
    "tgtColumnType": "string",
    "isDirected" : "true",
    "indices": [],
    "props": [
        {"name": "creatorId", "defaultValue": "", "dataType": "string"},
        {"name": "serviceName", "defaultValue": "", "dataType": "string"},
        {"name": "chatName", "defaultValue": "", "dataType": "string"}
    ],
    "consistencyLevel": "strong"
}'




curl -XPOST localhost:9000/chat/create/s2chat/shon/test
curl -XPOST localhost:9000/chat/create/s2chat/shon/test2
curl -XGET localhost:9000/chat/ids/s2chat/shon 
curl -XGET localhost:9000/chat/names/s2chat/shon
curl -XPOST localhost:9000/chat/join/s2chat/13524/jojo,rain
curl -XPOST localhost:9000/chat/join/s2chat/24888/elric,alek
curl -XGET localhost:9000/chat/id/s2chat/13524

curl -XPOST localhost:9000/chat/send/s2chat/jojo/13524 -H 'Content-Type: Application/json' -d '
[
    "hi this is first message",
    "나는 누구?", 
    "스토리는 망한듯"
]
'
curl -XPOST localhost:9000/chat/send/s2chat/alek/24888 -H 'Content-Type: Application/json' -d '
[
    "나는 alek",
    "쏘"
]
'

curl -XGET localhost:9000/chat/messages/s2chat/24888/0
curl -XGET localhost:9000/chat/messagesLs/s2chat/shon/0
curl -XPOST localhost:9000/chat/delete/s2chat/24888/30274
