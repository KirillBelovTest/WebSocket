(* ::Package:: *)

(* ::Chapter:: *)
(*WebSocket Handler*)


(* ::Program:: *)
(*+---------------------------------------------------+*)
(*|               WEBSSOCKET HANDLER                  |*)
(*|                                                   |*)
(*|                (reseive message)                  |*)
(*|                        |                          |*)
(*|               <check message type>                |*)
(*|              /         |          \               |*)
(*|   [handshake]       [frame]        [close frame]  |*)
(*|       |                |                 |        |*)
(*|  [send accept]     [decode]           {close}     |*)
(*|       |                |                          |*)
(*|    {to tcp}      [deserialize]                    |*)
(*|                        |                          |*)
(*|                 <select pipeline>                 |*)
(*|       /        /                 \       \        |*)
(*|     ..    [callback]         [subscribe]  ..      |*)
(*|                   \            /                  |*)
(*|                    <check type>                   |*)
(*|              null /            \ data             |*)
(*|             {next}            [serialize]         |*)
(*|                                    |              |*)
(*|                                 {to tcp}          |*)
(*+---------------------------------------------------+*)


(* ::Section::Closed:: *)
(*Requarements*)


Once[Map[If[Length[PacletFind[#]] === 0, PacletInstall[#]]&][{
    "KirillBelov/Internal", 
    "KirillBelov/Objects", 
    "KirillBelov/CSockets"
}]]; 


(*::Section::Close::*)
(*Begin package*)


BeginPackage["KirillBelov`WebSocket`", {
    "KirillBelov`Internal`Binary`", 
    "KirillBelov`Internal`Functions`", 
    "KirillBelov`Objects`", 
    "KirillBelov`CSockets`TCP`"
}]; 


(*::Section::Close::*)
(*Names*)


ClearAll["`*"]; 


WebSocketPacketQ::usage = 
"WebSocketPacketQ[packet] check that packet sent via WebSocket protocol."; 


WebSocketPacketLength::usage = 
"WSLength[packet] get expected message length."; 


WebSocketHandler::usage = 
"WebSocketHandler[opts] handle messages received via WebSocket protocol."; 


WebSocketSend::uasge = 
"WebSocketSend[connection, message] send message via WebSocket protocol."; 


WebSocketConnect::usage = 
"WebSocketConnect[host, port] connect to the remote server."; 


WebSocketConnection::usage = 
"WebSocketConnection[opts] representation of the client connection."; 


(*::Section::Close::*)
(*Begin private*)


Begin["`Private`"]; 


ClearAll["`*"]; 


Options[WebSocketConnect] = {
    "Serializer" -> ToString, 
    "Deserializer" -> ToExpression
};


WebSocketConnect[address_?StringQ, opts: OptionsPattern[]] := 
With[{
    parseAddress = URLParse[address], 
    serializer = OptionValue["Serializer"], 
    deserializer = OptionValue["Deserializer"]
}, 
    With[{socket = CSocketConnect[parseAddress["Domain"], If[# === None, 80, #]& @ parseAddress["Port"]]}, 
        BinaryWrite[socket, StringToByteArray[clientHandshake[URLBuild[parseAddress["Path"], parseAddress["Query"]]]]]; 

        WebSocketConnection[
            "Socket" -> socket, 
            "Serializer" -> serializer, 
            "Deserializer" -> deserializer
        ]
    ]
]; 


clientHandshake = StringTemplate["GET /`` HTTP/1.1\r\n
Host: example.com\r\n\
Upgrade: websocket\r\n\
Connection: Upgrade\r\n\
Sec-WebSocket-Key: <* uuid *>\r\n\
Sec-WebSocket-Version: 13\r\n\
\r\n"]; 


uuid := BaseEncode[BinarySerialize[CreateUUID[]], "Base64"]; 


WebSocketPacketQ[packet_Association] := 
frameQ[packet["SourceSocket"], packet["DataByteArray"]] || 
handshakeQ[packet["SourceSocket"], packet["DataByteArray"]]; 


WebSocketPacketLength[packet_Association] := 
If[frameQ[packet["SourceSocket"], packet["DataByteArray"]], 
    getFrameLength[packet["DataByteArray"]], 
    Length[packet["DataByteArray"]]
]; 


Options[WebSocketSend] = {
    "Serializer" -> $serializer
}; 


WebSocketSend[client_, message: _String | _ByteArray] := 
BinaryWrite[client, encodeFrame[message]]; 


WebSocketSend[client_, expr_, OptionsPattern[]] := 
Module[{serializer, message}, 
    serializer = OptionValue["Serializer"]; 
    message = serializer[expr]; 
    WebSocketSend[client, message]
]; 


CreateType[WebSocketHandler, init, {
    "MessageHandler" -> <||>, 
    "DefaultMessageHandler" -> $defaultMessageHandler, 	
    "Deserializer" -> $deserializer, (*Input: <|.., "Data" -> ByteArray[]|>*)
    "Serializer" -> $serializer, (*Return: ByteArray[]*) 
    "Connections", 
    "Buffer"
}]; 


handler_WebSocketHandler[packet_Association] /; packet["Event"] === "Received" := 
Module[{
    client = packet["SourceSocket"], 
    message = packet["Message"], 
    connections, deserializer, messageHandler, defaultMessageHandler, frame, buffer, data, expr
}, 
    connections = handler["Connections"]; 
    deserializer = handler["Deserializer"]; 
    Which[
        (*Return: Null*)
        closeQ[client, message], 
            $connections = Delete[$connections, Key[client]];
            connections["Remove", client]; 
            Map[Close, client["ConnectedClients"]];, 

        (*Return: ByteArray*)
        pingQ[client, message], 
            pong[client, message], 

        (*Return: Null*)
        frameQ[client, message], 
            frame = decodeFrame[message]; 
            ByteArrayToString[frame["Data"]]; 
            buffer = handler["Buffer"]; 

            If[
                frame["Fin"], 
                    deserializer = handler["Deserializer"]; 
                    data = getDataAndDropBuffer[buffer, client, frame]; 
                    expr = deserializer[data]; 
                    messageHandler = handler["MessageHandler"]; 
                    defaultMessageHandler = handler["DefaultMessageHandler"]; 
                    ConditionApply[messageHandler, defaultMessageHandler][client, expr];, 

                (*Else*) 
                    saveFrameToBuffer[buffer, client, frame]; 
            ];, 

        (*Return: _String*)
        handshakeQ[client, message], 
            connections["Insert", client]; 
            $connections[client] = connections; 
            handshake[client, message]
    ]
]; 


WebSocketHandler /: WebSocketSend[handler_WebSocketHandler, client_, message_] := 
WebSocketSend[client, encodeFrame[handler, message]]; 


WebSocketHandler /: WebSocketChannel[handler_WebSocketHandler, name_String, clients: {___}: {}] := 
Module[{channel}, 
    channel = WebSocketChannel[name, clients]; 
    channel["Serializer"] := handler["Serializer"]; 

    (*Return: WebSocketChannel[]*)
    channel
]; 


(*::Section::Close::*)
(*Internal*)


$guid = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"; 


$httpEndOfHead = StringToByteArray["\r\n\r\n"]; 


$defaultMessageHandler = Close@#&; 


$deserializer = ImportByteArray[#, "RawJSON"]&; 


$serializer = ExportByteArray[#, "RawJSON"]&; 


$directory = DirectoryName[$InputFileName, 2]; 


$connections = <||>; 


WebSocketHandler /: init[handler_WebSocketHandler] := (
    handler["Connections"] = CreateDataStructure["HashSet"]; 
    handler["Buffer"] = CreateDataStructure["HashTable"]; 
);


WebSocketChannel /: init[channel_WebSocketChannel] := 
channel["Connections"] = CreateDataStructure["HashSet"]; 


getConnetionsByClient[client_] := 
(*Return: DataStructure[HashSet]*)
$connections[client]; 


handshakeQ[client_, message_ByteArray] := 
Module[{head, connections}, 
    (*Result: DataStructure[HashSet]*)
    connections = getConnetionsByClient[client]; 
    head = ByteArrayToString[BytesSplit[message, $httpEndOfHead -> 1][[1]]]; 

    (*Return: True | False*)
    (!DataStructureQ[connections] || !connections["MemberQ", client]) && 
    Length[message] != StringLength[head] && 
    StringContainsQ[head, StartOfString ~~ "GET /"] && 
    StringContainsQ[head, StartOfLine ~~ "Upgrade: websocket"]
]; 


frameQ[client_, message_ByteArray] := 
Module[{connections}, 
    (*Result: DataStructure[HashSet]*)
    connections = getConnetionsByClient[client]; 

    (*Return: True | False*)
    DataStructureQ[connections] && connections["MemberQ", client]
]; 


closeQ[client_, message_ByteArray] := 
Module[{connections}, 
    (*Result: DataStructure[HashSet]*)
    connections = getConnetionsByClient[client]; 

    (*Return: True | False*)
    connections["MemberQ", client] && 
    FromDigits[IntegerDigits[message[[1]], 2, 8][[2 ;; ]], 2] == 8
]; 


pingQ[client_, message_ByteArray] := 
Module[{connections}, 
    (*Result: DataStructure[HashSet]*)
    connections = getConnetionsByClient[client]; 

    connections["MemberQ", client] && 
    FromDigits[IntegerDigits[message[[1]], 2, 8][[2 ;; ]], 2] == 9
]; 


pong[client_, message_ByteArray] := 
Module[{firstByte}, 
    firstByte = IntegerDigits[message[[1]], 2, 8]; 
    firstByte[[5 ;; 8]] = {1, 0, 1, 0}; 

    (*Return: ByteArray*)
    Join[ByteArray[{FromDigits[firstByte, 2]}], message[[2 ;; ]]]
]; 


handshake[client_, message_ByteArray] := 
Module[{messageString, key, acceptKey}, 
    messageString = ByteArrayToString[message]; 
    key = StringExtract[messageString, "Sec-WebSocket-Key: " -> 2, "\r\n" -> 1]; 
    acceptKey = createAcceptKey[key]; 
    
    (*Return: ByteArray[]*)
    StringToByteArray["HTTP/1.1 101 Switching Protocols\r\n" <> 
    "connection: upgrade\r\n" <> 
    "upgrade: websocket\r\n" <> 
    "content-type: text/html;charset=UTF-8\r\n" <> 
    "sec-websocket-accept: " <> acceptKey <> "\r\n\r\n"]
]; 


createAcceptKey[key_String] := 
(*Return: _String*)
BaseEncode[Hash[key <> $guid, "SHA1", "ByteArray"], "Base64"]; 


encodeFrame[message_ByteArray] := 
Module[{byte1, fin, opcode, length, mask, lengthBytes, reserved, maskingKey}, 
    fin = {1}; 
    
    reserved = {0, 0, 0}; 

    opcode = IntegerDigits[1, 2, 4]; 

    byte1 = ByteArray[{FromDigits[Join[fin, reserved, opcode], 2]}]; 

    length = Length[message]; 

    Which[
        length < 126, 
            lengthBytes = ByteArray[{128 + length}], 
        126 <= length < 2^16, 
            lengthBytes = ByteArray[Join[{128 + 126}, IntegerDigits[length, 256, 2]]], 
        2^16 <= length < 2^64, 
            lengthBytes = ByteArray[Join[{128 + 127}, IntegerDigits[length, 256, 8]]]
    ]; 

    maskingKey = RandomInteger[{0, 255}, 4]; 

    (*Return: _ByteArray*)
    ByteArray[Join[byte1, lengthBytes, maskingKey, unmask[maskingKey, message]]]
]; 


encodeFrame[message_String] := 
encodeFrame[StringToByteArray[message]]; 


WebSocketHandler /: encodeFrame[handler_WebSocketHandler, expr_] := 
Module[{serializer}, 
    serializer = handler["Serializer"]; 
    
    (*Return: ByteArray[]*)
    encodeFrame[serializer[expr]]
]; 


decodeFrame[message_ByteArray] := 
Module[{header, payload, data}, 
    header = getFrameHeader[message]; 
    payload = message[[header["PayloadPosition"]]]; 
    data = If[Length[header["MaskingKey"]] == 4, ByteArray[unmask[header["MaskingKey"], payload]], payload]; 
    (*Return: _Association*)
    Append[header, "Data" -> data]
]; 


getFrameLength[client_, message_ByteArray] := 
Module[{length}, 
    length = FromDigits[IntegerDigits[message[[2]], 2, 8][[2 ;; ]], 2]; 

    Which[
        length == 126, length = FromDigits[Normal[message[[3 ;; 4]]], 256] + 8, 
        length == 127, length = FromDigits[Normal[message[[3 ;; 10]]], 256] + 14, 
        True, length = length + 6
    ]; 

    (*Return: _Integer*)
    length
]; 


getFrameHeader[message_ByteArray] := 
Module[{byte1, byte2, fin, opcode, mask, len, maskingKey, nextPosition, payload, data}, 
    byte1 = IntegerDigits[message[[1]], 2, 8]; 
    byte2 = IntegerDigits[message[[2]], 2, 8]; 

    fin = byte1[[1]] === 1; 
    opcode = Switch[FromDigits[byte1[[2 ;; ]], 2], 
        1, "Part", 
        2, "Text", 
        4, "Binary", 
        8, "Close"
    ]; 

    mask = byte2[[1]] === 1; 

    len = FromDigits[byte2[[2 ;; ]], 2]; 

    nextPosition = 3; 

    Which[
        len == 126, len = FromDigits[Normal[message[[3 ;; 4]]], 256]; nextPosition = 5, 
        len == 127, len = FromDigits[Normal[message[[3 ;; 10]]], 256]; nextPosition = 11
    ]; 

    If[mask, 
        maskingKey = message[[nextPosition ;; nextPosition + 3]]; nextPosition = nextPosition + 4, 
        maskingKey = {}
    ]; 

    (*Return: _Association*)
    <|
        "Fin" -> fin, 
        "OpCode" -> opcode, 
        "Mask" -> mask, 
        "Len" -> len, 
        "MaskingKey" -> maskingKey, 
        "PayloadPosition" -> nextPosition ;; nextPosition + len - 1
    |>
]; 


unmaskCompiled = Compile[{{maskingKey, _Integer, 1}, {payload, _Integer, 1}},
Table[
    BitXor[payload[[i]], maskingKey[[Mod[i - 1, 4] + 1]]], {i, 1, Length[payload]}], 
    Parallelization -> True, 
    RuntimeAttributes -> {Listable}, 
    CompilationOptions -> {"ExpressionOptimization" -> True}, 
   "RuntimeOptions" -> "Speed"
]; 


unmask[maskingKey_, payload_] := 
ByteArray[unmaskCompiled[Normal[maskingKey], Normal[payload]]]; 


saveFrameToBuffer[buffer_DataStructure, client: _[uuid_], frame_] := 
Module[{clientBuffer}, 
    If[buffer["KeyExistsQ", uuid], 
        clientBuffer = buffer["Lookup", uuid]; 
        clientBuffer["Append", frame]; , 
    (*Else*)
        clientBuffer = CreateDataStructure["DynamicArray", {frame}]; 
        buffer["Insert", uuid -> clientBuffer]; 
    ]; 
    (*Return: Null*)
]; 


getDataAndDropBuffer[buffer_DataStructure, client: _[uuid_], frame_] := 
Module[{fragments, clientBuffer}, 
    If[buffer["KeyExistsQ", uuid], 
        clientBuffer = buffer["Lookup", uuid]; 
        If[clientBuffer["Length"] > 0, 
            fragments = Append[clientBuffer["Elements"], frame][[All, "Data"]]; 
            clientBuffer["DropAll"]; 
            (*Return: ByteArray[]*)
            Apply[Join, fragments], 
        (*Else*)
            (*Return: ByteArray[]*)
            frame["Data"]
        ], 
    (*Else*)
        (*Return: ByteArray[]*)
        frame["Data"]
    ]
]; 


(*::Section::Close::*)
(*End private*)


End[]; 


(*::Section::Close::*)
(*End package*)


EndPackage[]; 