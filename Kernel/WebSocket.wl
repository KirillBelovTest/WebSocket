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
With[{client = packet["SourceSocket"], message = packet["DataByteArray"]}, 
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
    If[clientConnectedQ[client],
        (*Return: _Integer*) 
        getFrameLength[data], 
    (*Else*) 
        (*Return: _Integer*)
        Length[data]
    ]
]; 


Options[WebSocketSend] = {
    "Serializer" -> Automatic, 
    "FrameType" -> Automatic, 
    "Masking" -> Automatic
}; 


WebSocketSend[client_, message: _String | _ByteArray, OptionsPattern[]] := 
Modle[{serializer, masking, frameType}, 
    If[clientConnectedQ[client] && OptionValue["Serializer"] === Automatic, 
        serializer = $connedctedClients[client]["Serializer"], 
    (*Else*)
        serializer = If[]
    ]
]
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


(* ::Section::Close:: *)
(*WebSocketConnect*)


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


(*::Section::Close::*)
(*Internal*)


clientHandshakeTemplate = StringTemplate["GET `` HTTP/1.1\r\n\
Host: example.com\r\n\
Upgrade: websocket\r\n\
Connection: Upgrade\r\n\
Sec-WebSocket-Key: <* $clientHandshakeUUID *>\r\n\
Sec-WebSocket-Version: 13\r\n\
\r\n"]; 


$clientHandshakeUUID := BaseEncode[BinarySerialize[CreateUUID[]], "Base64"]; 


$guid = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"; 


$httpEndOfHead = StringToByteArray["\r\n\r\n"]; 


$directory = DirectoryName[$InputFileName, 2]; 


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
    If[Lenggth[headBytes] === Length[message], 
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
Module[{message, key, acceptKey}, 
    message = ByteArrayToString[data]; 
    key = StringTrim[StringCases[message, "Sec-WebSocket-Key: " ~~ k__ ~~ EndOfLine :> k][[1]]]; 
    acceptKey = BaseEncode[Hash[key <> $guid, "SHA1", "ByteArray"], "Base64"]; 
    
    (*Return: ByteArray[]*)
    StringToByteArray[
        "HTTP/1.1 101 Switching Protocols\r\n" <> 
        "connection: upgrade\r\n" <> 
        "upgrade: websocket\r\n" <> 
        "content-type: text/html;charset=UTF-8\r\n" <> 
        "sec-websocket-accept: " <> acceptKey <> 
        "\r\n" <> 
        "\r\n"
    ]
]; 


(* ::Section::Close:: *)
(*Close*)


closeQ[client_, message_ByteArray] := 
Module[{connections}, 
    (*Result: DataStructure[HashSet]*)
    connections = getConnectionsByClient[client]; 

    (*Return: True | False*)
    connections["MemberQ", client] && 
    FromDigits[IntegerDigits[message[[1]], 2, 8][[2 ;; ]], 2] == 8
]; 


(* ::Section::Close:: *)
(*Ping-Pong*)


pingQ[client_, message_ByteArray] := 
Module[{connections}, 
    (*Result: DataStructure[HashSet]*)
    connections = getConnectionsByClient[client]; 

    connections["MemberQ", client] && 
    FromDigits[IntegerDigits[message[[1]], 2, 8][[2 ;; ]], 2] == 9
]; 


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
        1, "Part", 
        2, "Text", 
        4, "Binary", 
        8, "Close", 
        _, "Unexpected"
    ]; 

    masking = byte2[[1]] === 1; 

    lenByte = FromDigits[byte2[[2 ;; ]], 2]; 

    nextPosition = 3; 

    Which[
        lenByte == 126, len = FromDigits[Normal[frame[[3 ;; 4]]], 256]; nextPosition = 5, 
        lenByte == 127, len = FromDigits[Normal[frame[[3 ;; 10]]], 256]; nextPosition = 11
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
            fragments = Append[clientBuffer["Elements"], frame][[All, "DataByteArray"]]; 
            clientBuffer["DropAll"]; 
            (*Return: ByteArray[]*)
            Apply[Join, fragments], 
        (*Else*)
            (*Return: ByteArray[]*)
            frame["DataByteArray"]
        ], 
    (*Else*)
        (*Return: ByteArray[]*)
        frame["DataByteArray"]
    ]
]; 


(*::Section::Close::*)
(*End private*)


End[]; 


(*::Section::Close::*)
(*End package*)


EndPackage[]; 