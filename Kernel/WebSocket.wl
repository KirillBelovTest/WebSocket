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


WebSocketFrame::usage = 
"WebSocketFrame[payload] create unmasked frame with specifyed payload.";


WebSocketServerCreate::usage = 
"WebSocketServerCreate[port, handlers] returns prepared web socket server.";


(*::Section::Close::*)
(*Begin private*)


Begin["`Private`"]; 


ClearAll["`*"]; 


(*::Section::Close::*)
(*WebSocketPacketQ*)


WebSocketPacketQ[packet_Association] := 
With[{
    client = packet["SourceSocket"], 
    message = packet["DataByteArray"]
}, 
    (*Return: True | False*)
    clientConnectedQ[client] || 
    handshakeQ[message]
]; 


(*::Section::Close::*)
(*WebSocketPacketLength*)


WebSocketPacketLength[packet_Association] := 
With[{
    client = packet["SourceSocket"], 
    data = packet["DataByteArray"]
},
    (*Return: _Integer?Positive*)
    If[clientConnectedQ[client],
        getFrameLength[data], 
    (*Else*) 
        Length[data]
    ]
]; 


Options[WebSocketSend] = {
    "Serializer" -> Automatic
}; 


WebSocketSend::cantsend = 
"Expr not serialized.";


WebSocketSend[client_, message: _String | _ByteArray, OptionsPattern[]] := 
BinaryWrite[client, encodeFrame[message, !clientConnectedQ[client]]];


WebSocketSend[client_, expr_, opts: OptionsPattern[]] := 
Module[{serializer, message}, 
    serializer = Which[
        # =!= Automatic, #, 
        !clientConnectedQ[client], $defaultSerializer, 
        True, $connectedClients[client]["Serializer"]
    ]& @ OptionValue["Serializer"]; 
    
    message = serializer[expr]; 

    If[StringQ[message] || ByteArrayQ[message],  
        WebSocketSend[client, message], 
    (*Else*)
        Message[WebSocketSend::cantsend];
    ]
]; 


CreateType[WebSocketHandler, init, {
    "MessageHandler" -> <||>, 
    "Deserializer" -> Identity, 
    "Serializer" -> ToString, 
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
    deserializer, defaultMessageHandler, messageHandler, frame, buffer, data, expr
}, 
    Which[
        (*Return: Null*)
        closeQ[client, message], 
            KeyDropFrom[$connectedClients, client];
            connections["Remove", client]; 
            Close[client];, 

        (*Return: ByteArray*)
        pingQ[client, message], 
            pong[message], 

        (*Return: Null*)
        clientConnectedQ[client], 
            buffer = handler["Buffer"]; 
            frame = decodeFrame[message]; (*<|"Fin"->..,"Opcode"->..,"Payload"->..|>*)

            If[
                frame["Fin"], 
                    deserializer = Switch[frame["Opcode"], 
                        "Text", handler["Deserializer"] @* ByteArrayToString, 
                        "Binary", handler["Deserializer"], 
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
        handshakeQ[message], 
            Echo[ByteArrayToString[message], "Handshake received: "];
            $connectedClients[client] = handler; 
            connections["Insert", client]; 
            handshake[message]
    ]
]; 


Options[WebSocketServerCreate] = {
    "Serializer" -> ToString, 
    "Deserializer" -> Identity, 
    "DefaultMessageHandler" -> Function[Null]
};


WebSocketServerCreate[host: _String: "localhost", port: _Integer?Positive, name: _String: "WebSocket", messageHandler_?AssociationQ, opts: OptionsPattern[]] := 
With[{
    serverSocket = CSocketOpen[host, port], 
    serverHandler = CSocketHandler[]
}, 
    With[{
        listener = SocketListen[serverSocket, serverHandler], 
        webSocketHandler = WebSocketHandler[opts]
    }, 
        webSocketHandler["Listener"] = listener;

        serverHandler["Accumulator", name] = WebSocketPacketQ -> WebSocketPacketLength;
        serverHandler["Handler", name] = WebSocketPacketQ -> Function[webSocketHandler[#]];

        webSocketHandler["MessageHandler"] = messageHandler; 

        (*Return*)
        webSocketHandler
    ]
];


(* ::Section::Close:: *)
(*WebSocketConnect*)


CreateType[WebSocketConnection, {
    "Address", 
    "Headers" -> <||>,
    "Socket", 
    "Serializer" -> $defaultSerializer,
    "Deserializer" -> $defaultDeserializer
}]; 


Options[WebSocketConnect] = {
    "Headers" -> <||>, 
    "Serializer" -> $defaultSerializer,
    "Deserializer" -> $defaultDeserializer
};


WebSocketConnect[address_?StringQ, opts: OptionsPattern[]] := 
With[{parsedAddress = URLParse[address]}, 
    With[{
        host = parsedAddress["Domain"], 
        port = If[# === None, 80, #]& @ parsedAddress["Port"], 
        path = "/" <> StringTrim[URLBuild[parsedAddress["Path"], parsedAddress["Query"]], "/"], 
        headers = If[# === "", #, # <> "\r\n"]& @ StringRiffle[KeyValueMap[#1 <> ": " <> #2&, OptionValue["Headers"]], "\r\n"]
    }, 
        With[{socket = CSocketConnect[host, port]}, 
            WriteString[socket, clientHandshakeTemplate[path, headers]]; 

            TimeConstrained[
                While[!SocketReadyQ[socket], Pause[0.01]], 
                5, 
                Return[$Failed]
            ];

            Module[{responseMessage = ByteArray[{}]}, 
                TimeConstrained[
                    While[SocketReadyQ[socket], 
                        responseMessage = Join[responseMessage, SocketReadMessage[socket]]
                    ]; 
                    Echo[ByteArrayToString[responseMessage]];, 
                    5, 
                    $Failed
                ];
            ];

            WebSocketConnection[
                "Address" -> address, 
                "Socket" -> socket, 
                opts
            ]
        ]
    ]
]; 


WebSocketConnection /: WebSocketSend[connection_WebSocketConnection, message_] := 
WebSocketSend[connection["Socket"], message, "Serializer" -> connection["Serializer"]];


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


(*::Section::Close::*)
(*Internal*)


clientHandshakeTemplate = StringTemplate["GET `1` HTTP/1.1\r\n\
Host: example.com\r\n\
Upgrade: websocket\r\n\
Connection: Upgrade\r\n\
Sec-WebSocket-Key: <* $clientHandshakeUUID *>\r\n\
Sec-WebSocket-Version: 13\r\n\
`2`\
\r\n"]; 


$clientHandshakeUUID := BaseEncode[BinarySerialize[CreateUUID[]], "Base64"]; 


$guid = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"; 


$httpEndOfHead = StringToByteArray["\r\n\r\n"]; 


$directory = DirectoryName[$InputFileName, 2]; 


$defaultSerializer = ToString;


$defaultDeserializer = Identity;


If[!AssociationQ[$connectedClients], $connectedClients = <||>]; 


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


clientConnectedQ[client_] := 
KeyExistsQ[$connectedClients, client]; 


(* ::Section::Close:: *)
(*Handshake*)


handshakeQ[message_ByteArray] := 
Module[{headBytes, head, connections}, 
    headBytes = BytesSplit[message, $httpEndOfHead -> 1][[1]]; 
    If[Length[headBytes] === Length[message], 
        Return[False], 
    (*Else*)
        head = ByteArrayToString[headBytes]; 
        
        (*Return: True | False*)
        Length[message] != StringLength[head] && 
        StringContainsQ[head, StartOfString ~~ "GET /", IgnoreCase -> True] && 
        StringContainsQ[head, StartOfLine ~~ "Upgrade: websocket", IgnoreCase -> True]
    ]
]; 


handshake[data_ByteArray] := 
Module[{message, key, acceptKey, response}, 
    message = ByteArrayToString[data]; 
    key = StringTrim[StringCases[message, Shortest["Sec-WebSocket-Key: " ~~ k__ ~~ "\r\n"] :> k][[1]]]; 
    acceptKey = BaseEncode[Hash[key <> $guid, "SHA1", "ByteArray"], "Base64"]; 
    
    (*Return: _String*)
    "HTTP/1.1 101 Switching Protocols\r\n" <> 
    "Upgrade: websocket\r\n" <> 
    "Connection: Upgrade\r\n" <> 
    "Sec-WebSocket-Accept: " <> acceptKey <> "\r\n" <> 
    "\r\n"
]; 


(* ::Section::Close:: *)
(*Close*)


closeQ[client_, message_ByteArray] := 
clientConnectedQ[client] && 
FromDigits[IntegerDigits[message[[1]], 2, 8][[2 ;; ]], 2] == 8; 


(* ::Section::Close:: *)
(*Ping-Pong*)


pingQ[client_, message_ByteArray] := 
clientConnectedQ[client] && 
FromDigits[IntegerDigits[message[[1]], 2, 8][[2 ;; ]], 2] == 9; 


pong[data_ByteArray] := 
Module[{firstByte}, 
    firstByte = IntegerDigits[data[[1]], 2, 8]; 
    firstByte[[5 ;; 8]] = {1, 0, 1, 0}; 

    (*Return: ByteArray*)
    Join[ByteArray[{FromDigits[firstByte, 2]}], data[[2 ;; ]]]
]; 


(* ::Section::Close:: *)
(*Framing*)


encodeFrame[message_ByteArray, opcode: 0 | 1 | 2: 2, masking: True | False: True] := 
Module[{byte1, fin, length, mask, lengthBytes, reserved, maskBit, maskingKey, opcodeBytes}, 
    fin = {UnitStep[opcode - 1]}; 
    
    maskBit = If[masking, 1, 0];

    reserved = {0, 0, 0}; 

    opcodeBytes = IntegerDigits[opcode, 2, 4]; 

    byte1 = ByteArray[{FromDigits[Join[fin, reserved, opcodeBytes], 2]}]; 

    length = Length[message]; 

    lengthBytes = Which[
        length < 126, 
            ByteArray[{maskBit * 128 + length}], 
        126 <= length < 2^16, 
            ByteArray[Join[{maskBit * 128 + 126}, IntegerDigits[length, 256, 2]]], 
        2^16 <= length < 2^64, 
            ByteArray[Join[{maskBit * 128 + 127}, IntegerDigits[length, 256, 8]]]
    ]; 

    (*Return: ByteArray[]*)
    If[masking, 
        maskingKey = RandomInteger[{0, 255}, 4]; 
        Join[ByteArray[Join[byte1, lengthBytes, maskingKey]], unmask[maskingKey, message]], 
    (*Else*)
        Join[ByteArray[Join[byte1, lengthBytes]], message]
    ]
]; 


encodeFrame[message_String, opcode: 0 | 1 | 2: 1, masking: True | False: True] := 
encodeFrame[StringToByteArray[message], opcode, masking]; 

(*Returns: <|
    "Payload" -> ByteArray[],
    "Fin" -> True | False,
    "Opcode" -> "Text" | "Binary" | "Close" | "Ping" | "Pong" | "Part" | "Unexpected",
    "Masking" -> True | False,
    "MaskingKey" -> ByteArray[],
    "Length" -> _Integer,
    "PayloadPosition" -> _Integer
|>*)
decodeFrame[frame_ByteArray] := 
Module[{header, payloadPosition, payload}, 
    header = getFrameHeader[frame]; 
    payloadPosition = header["PayloadPosition"]; 
    payload = If[header["Masking"], 
        unmask[header["MaskingKey"], frame[[payloadPosition]]], 
    (*Else*)
        frame[[payloadPosition]]
    ]; 

    (*Return: _Association*)
    Append[header, "Payload" -> payload]
]; 


getFrameLength[message_ByteArray] := 
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


getFrameHeader[frame_ByteArray] := 
Module[{
    byte1, byte2, fin, opcode, masking, lenByte, len, 
    maskingKey, nextPosition, payload, data
}, 
    byte1 = IntegerDigits[frame[[1]], 2, 8]; 
    byte2 = IntegerDigits[frame[[2]], 2, 8]; 

    fin = byte1[[1]] === 1; 
    opcode = Switch[FromDigits[byte1[[2 ;; ]], 2], 
        0, "Part", 
        1, "Text", 
        2, "Binary", 
        _, "Unexpected"
    ]; 

    masking = byte2[[1]] === 1; 

    lenByte = FromDigits[byte2[[2 ;; ]], 2]; 

    nextPosition = 3; 

    len = Which[
        lenByte < 126, lenByte, 
        lenByte == 126, FromDigits[Normal[frame[[3 ;; 4]]], 256]; nextPosition = 5, 
        lenByte == 127, FromDigits[Normal[frame[[3 ;; 10]]], 256]; nextPosition = 11
    ]; 

    If[masking, 
        maskingKey = frame[[nextPosition ;; nextPosition + 3]]; 
        nextPosition = nextPosition + 4, 
    (*Else*)
        maskingKey = ByteArray[{}]
    ]; 

    (*Return: _Association*)
    <|
        "Fin" -> fin, 
        "Opcode" -> opcode, 
        "Masking" -> masking, 
        "Length" -> len, 
        "MaskingKey" -> maskingKey, 
        "PayloadPosition" -> nextPosition ;; (nextPosition + len - 1)
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
            fragments = Append[clientBuffer["Elements"], frame][[All, "Payload"]]; 
            clientBuffer["DropAll"]; 
            (*Return: ByteArray[]*)
            Apply[Join, fragments], 
        (*Else*)
            (*Return: ByteArray[]*)
            frame["Payload"]
        ], 
    (*Else*)
        (*Return: ByteArray[]*)
        frame["Payload"]
    ]
]; 


(*::Section::Close::*)
(*End private*)


End[]; 


(*::Section::Close::*)
(*End package*)


EndPackage[]; 