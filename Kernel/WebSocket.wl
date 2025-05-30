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
    "KirillBelov`CSockets`TCP`", 
    "KirillBelov`CSockets`Handler`"
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


WebSocketServerCreate::usage = 
"WebSocketServerCreate[port, handlers] returns prepared web socket server.";


(*::Section::Close::*)
(*Begin private*)


Begin["`Private`"]; 


ClearAll["`*"]; 


(* WebSocketConnect *)


Options[WebSocketConnect] = {
    "Serializer" -> Function[ExportByteArray[#, "RawJSON"]], 
    "Deserializer" -> Function[ImportByteArray[#, "RawJSON"]]
};


CreateType[WebSocketConnection, {
    "Address", 
    "Socket", 
    "TextSerializer" -> ToString, 
    "BinarySerializer" -> BinarySerialize, 
    "TextDeserializer" -> Identity, 
    "BinaryDeserializer" -> BinaryDeserialize
}]; 


Options[WebSocketConnect] = {
    "TextSerializer" -> ToString, 
    "BinarySerializer" -> BinarySerialize, 
    "TextDeserializer" -> Identity, 
    "BinaryDeserializer" -> BinaryDeserialize
};


WebSocketConnect[address_?StringQ, opts: OptionsPattern[]] := 
With[{parsedAddress = URLParse[address]}, 
    With[{
        host = parsedAddress["Domain"], 
        port = If[# === None, 80, #]& @ parsedAddress["Port"], 
        path = "/" <> StringTrim[URLBuild[parsedAddress["Path"], parsedAddress["Query"]], "/"]    
    }, 
        With[{socket = CSocketConnect[host, port]}, 
            WriteString[socket, clientHandshakeTemplate[path]]; 

            WebSocketConnection[
                "Address" -> address, 
                "Socket" -> socket, 
                opts
            ]
        ]
    ]
]; 


WebSocketConnection /: BinaryWrite[connection_WebSocketConnection, message_ByteArray] := 
WebSocketSend[connection["Socket"], message, "Masking" -> True, "Serializer" -> connection["Serializer"]];


WebSocketConnection /: WriteString[connection_WebSocketConnection, message_] := 
WebSocketSend[connection["Socket"], message, "Masking" -> True, "Serializer" -> connection["Serializer"]];


WebSocketConnection /: WebSocketSend[connection_WebSocketConnection, message_] := 
WebSocketSend[connection["Socket"], message, "Masking" -> True, "Serializer" -> connection["Serializer"]];


WebSocketConnection /: SocketReadyQ[connection_WebSocketConnection, timeout: _?NumericQ: 0] := 
SocketReadyQ[connection["Socket"], timeout];


WebSocketConnection /: SocketReadMessage[connection_WebSocketConnection] := 
Module[{message = ByteArray[{}]}, 
    If[!SocketReadyQ[connection], 
        (*Return: $Failed*)
        $Failed, 
    (*Else*) 
        (*Return: _ByteArray*)
        While[SocketReadyQ[connection, 0.01], 
            message = Join[message, SocketReadMessage[connection["Socket"]]];
        ]; 
        
        (* Return: ByteArray[] *)
        connection["Deserializer"][message]
    ]
];


clientHandshakeTemplate = StringTemplate["GET `` HTTP/1.1\r\n\
Host: example.com\r\n\
Upgrade: websocket\r\n\
Connection: Upgrade\r\n\
Sec-WebSocket-Key: <* $clientHandshakeUUID *>\r\n\
Sec-WebSocket-Version: 13\r\n\
\r\n"]; 


$clientHandshakeUUID := BaseEncode[BinarySerialize[CreateUUID[]], "Base64"]; 


WebSocketPacketQ[packet_Association] := 
With[{client = packet["SourceSocket"], message = packet["DataByteArray"]}, 
    (*Return: True | False*)
    frameQ[client, message] || 
    handshakeQ[client, message]
]; 


WebSocketPacketLength[packet_Association] := 
With[{client = packet["SourceSocket"], message = packet["DataByteArray"]}, 
    If[frameQ[client, message],
        (*Return: _Integer*) 
        getFrameLength[client, message], 
    (*Else*) 
        (*Return: _Integer*)
        Length[packet["DataByteArray"]]
    ]
]; 


Options[WebSocketSend] = {
    "Serializer" -> Function[ExportByteArray[#, "RawJSON"]], 
    "Masking" -> True
}; 


WebSocketSend[client_, message: _String | _ByteArray, OptionsPattern[]] := 
BinaryWrite[client, 
    If[OptionValue["Masking"], 
        encodeFrame[message], 
    (*Else*)
        encodeFrameServer[message]
    ]
]; 


WebSocketSend[client_, expr_, opts: OptionsPattern[]] := 
Module[{serializer, message}, 
    serializer = OptionValue["Serializer"]; 
    message = serializer[expr]; 
    WebSocketSend[client, message, opts]
]; 


CreateType[WebSocketHandler, init, {
    "MessageHandler" -> <||>, 
    "DefaultMessageHandler" -> Function[Null], 
    "BinaryDeserializer" -> BinaryDeserialize, 
    "TextDeserializer" -> Identity, 
    "BinarySerializer" -> BinarySerialize, (*Return: ByteArray[]*) 
    "TextSerializer" -> ToString, (*Return: _String*) 
    "Connections", 
    "Buffer"
}]; 


WebSocketHandler /: init[handler_WebSocketHandler] := (
    handler["Connections"] = CreateDataStructure["HashSet"]; 
    handler["Buffer"] = CreateDataStructure["HashTable"]; 
);


handler_WebSocketHandler[packet_Association?dataPacketQ] := 
Module[{
    client = packet["SourceSocket"], 
    message = packet["DataByteArray"], 
    connections = handler["Connections"], 
    deserializer, messageHandler, defaultMessageHandler, frame, buffer, data, expr
}, 
    Which[
        (*Return: Null*)
        closeQ[client, message], 
            KeyDropFrom[$connections, client];
            connections["Remove", client]; 
            close[client, message];
            Close[client], 

        (*Return: ByteArray*)
        pingQ[client, message], 
            pong[client, message], 

        (*Return: Null*)
        frameQ[client, message], 
            buffer = handler["Buffer"]; 
            frame = decodeFrame[message]; (*<|"Fin"->..,"OpCode"->..,"Data"->..|>*)

            If[
                frame["Fin"], 
                    deserializer = Switch[frame["OpCode"], 
                        "Text", handler["TextDeserializer"] @* ByteArrayToString, 
                        "Binary", handler["BinaryDeserializer"], 
                        _, Identity
                    ]; 

                    data = getDataAndDropBuffer[buffer, client, frame]; 
                    expr = deserializer[data]; 

                    messageHandler = handler["MessageHandler"]; 
                    defaultMessageHandler = handler["DefaultMessageHandler"]; 
                    
                    (*Returns Null! Don't send the result. Specific handler itself must decide to send data*)
                    ConditionApply[messageHandler, defaultMessageHandler][client, expr];, 

                (*Else*) 
                    saveFrameToBuffer[buffer, client, frame]; 
            ];, 

        (*Return: _String*)
        handshakeQ[client, message], 
            $connections[client] = connections; 
            connections["Insert", client]; 
            handshake[client, message]
    ]
]; 


WebSocketHandler /: WebSocketSend[handler_WebSocketHandler, client_, message_] := 
WebSocketSend[client, encodeFrame[handler, message]]; 


Options[CreateWebSocketServer] = {
    "BinarySerializer" -> BinarySerialize, 
    "TextSerializer" -> ToString, 
    "BinaryDeserializer" -> BinaryDeserialize, 
    "TextDeserializer" -> Identity
};


CreateWebSocketServer[host: _String: "localhost", port_Integer, name_String: "WebSocket", messageHandlers_?AssociationQ, opts: OptionsPattern[]] := 
With[{
    serverSocket = CSocketOpen[host, port], 
    serverHandler = CSocketHandler[], 
    binaryDeserializer = OptionValue[]
}, 
    With[{
        listener = SocketListen[serverSocket, serverHandler], 
        webSocketHandler = WebSocketHandler[opts]
    }, 
        webSocketHandler["Listener"] = listener;

        serverHandler["Accumulator", name] = WebSocketPacketQ -> WebSocketPacketLength;
        serverHandler["Handler", name] = WebSocketPacketQ -> webSocketHandler;

        webSocketHandler["MessageHandler"] = messageHandlers; 

        (*Return*)
        webSocketHandler
    ]
];


(*::Section::Close::*)
(*Internal*)


$guid = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"; 


$httpEndOfHead = StringToByteArray["\r\n\r\n"]; 


$directory = DirectoryName[$InputFileName, 2]; 


If[!AssociationQ[$connections], $connections = <||>]; 


With[{$emptyHashSet = CreateDataStructure["HashSet"]},
    getConnectionsByClient[client_] := 
    If[KeyExistsQ[$connections, client], 
        $connections[client], 
    (*Else*)
        $emptyHashSet
    ]
]; 


dataPacketQ[packet_Association] := 
KeyExistsQ[packet, "DataByteArray"] && ByteArrayQ[packet["DataByteArray"]];


handshakeQ[client_, message_ByteArray] := 
Module[{head, connections}, 
    head = ByteArrayToString[BytesSplit[message, $httpEndOfHead -> 1][[1]]]; 

    (*Return: True | False*)
    Length[message] != StringLength[head] && 
    StringContainsQ[head, StartOfString ~~ "GET /", IgnoreCase -> True] && 
    StringContainsQ[head, StartOfLine ~~ "Upgrade: websocket", IgnoreCase -> True]
]; 


frameQ[client_, message_ByteArray] := 
Module[{connections}, 
    (*Result: DataStructure[HashSet]*)
    connections = getConnectionsByClient[client]; 

    (*Return: True | False*)
    DataStructureQ[connections] && connections["MemberQ", client]
]; 


closeQ[client_, message_ByteArray] := 
Module[{connections}, 
    (*Result: DataStructure[HashSet]*)
    connections = getConnectionsByClient[client]; 

    (*Return: True | False*)
    connections["MemberQ", client] && 
    FromDigits[IntegerDigits[message[[1]], 2, 8][[2 ;; ]], 2] == 8
]; 


pingQ[client_, message_ByteArray] := 
Module[{connections}, 
    (*Result: DataStructure[HashSet]*)
    connections = getConnectionsByClient[client]; 

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
    result = StringToByteArray["HTTP/1.1 101 Switching Protocols\r\n" <> 
    "connection: upgrade\r\n" <> 
    "upgrade: websocket\r\n" <> 
    "content-type: text/html;charset=UTF-8\r\n" <> 
    "sec-websocket-accept: " <> acceptKey <> "\r\n\r\n"]; 

    BinaryWrite[client, result]; 
]; 


createAcceptKey[key_String] := 
(*Return: _String*)
BaseEncode[Hash[key <> $guid, "SHA1", "ByteArray"], "Base64"]; 


encodeFrame[message_ByteArray, opcode: 0 | 1 | 2: 2, masking: True | False: True] := 
Module[{byte1, fin, length, mask, lengthBytes, reserved, maskingKey}, 
    fin = {UnitStep[opcode - 1]}; 
    
    reserved = {0, 0, 0}; 

    opcodeBytes = IntegerDigits[opcode, 2, 4]; 

    byte1 = ByteArray[{FromDigits[Join[fin, reserved, opcode], 2]}]; 

    length = Length[message]; 

    lengthBytes = Which[
        length < 126, 
            ByteArray[{masking * 128 + length}], 
        126 <= length < 2^16, 
            ByteArray[Join[{masking * 128 + 126}, IntegerDigits[length, 256, 2]]], 
        2^16 <= length < 2^64, 
            ByteArray[Join[{masking * 128 + 127}, IntegerDigits[length, 256, 8]]]
    ]; 

    If[masking, 
        maskingKey = RandomInteger[{0, 255}, 4]; 

        (*Return: _ByteArray*)
        Join[ByteArray[Join[byte1, lengthBytes, maskingKey]], unmask[maskingKey, message]], 
    (*Else*)
        (*Return: _ByteArray*)
        Join[ByteArray[Join[byte1, lengthBytes]], message]
    ]
]; 


encodeFrame[message_String, opcode: 0 | 1 | 2: 1, masking: True | False: True] := 
encodeFrame[StringToByteArray[message], opcode, masking]; 


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
        8, "Close", 
        _, "Unexpcted"
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