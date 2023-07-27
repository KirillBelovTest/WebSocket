(* ::Package:: *)

(* ::Chapter:: *)
(*WS Handler*)


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


(*::Section::Close::*)
(*Begin package*)


BeginPackage["KirillBelov`WebSocketHandler`", {"KirillBelov`Internal`", "KirillBelov`Objects`", "KirillBelov`CSocketListener`"}]; 


(*::Section::Close::*)
(*Names*)


ClearAll["`*"]; 


$CurrenctClient::usage = 
"Current SocketObject."; 


WebSocketPacketQ::usage = 
"WebSocketPacketQ[client, packet] check that packet sent via WebSocket protocol."; 


WebSocketPacketLength::usage = 
"WSLength[client, message] get expected message length."; 


WebSocketSend::uasge = 
"WebSocketSend[client, message] send message via WebSocket protocol."; 


WebSocketChannel::usage = 
"WebSocketChannel[name] multiple client connection."; 


WebSocketHandler::usage = 
"WebSocketHandler[opts] handle messages received via WebSocket protocol."; 


(*::Section::Close::*)
(*Begin private*)


Begin["`Private`"]; 


ClearAll["`*"]; 


WebSocketPacketQ[client: _SocketObject | _CSocket, message_ByteArray] := 
(frameQ[client, message] || handshakeQ[client, message]); 


WebSocketPacketLength[client: _SocketObject | _CSocket, message_ByteArray] := 
If[frameQ[client, message], 
	getFrameLength[client, message], 
	Length[message]
]; 


Options[WebSocketSend] = {
	"Serializer" -> $serializer
}


WebSocketSend[client: _SocketObject | _CSocket, message: _String | _ByteArray] := 
BinaryWrite[client, encodeFrame[message]]; 


WebSocketSend[client: _SocketObject | _CSocket, expr_, OptionsPattern[]] := 
Module[{serializer, message}, 
	serializer = OptionValue["Serializer"]; 
	message = serializer[expr]; 
	WebSocketSend[client, message]
]; 


CreateType[WebSocketChannel, init, {
	"Name", 
	"Serializer" -> $serializer, 
	"Connections"
}]; 


WebSocketChannel[name_String, clients: {___SocketObject | ___CSocket}: {}, serializer_: $serializer] := 
Module[{channel, connections}, 
	channel = WebSocketChannel["Name" -> name, "Serializer" -> serializer]; 
	connections = channel["Connections"]; 
	Map[connections["Insert", #]&, clients]; 
	
	(*Return: WebSocketChannel[]*)
	channel
]; 


WebSocketChannel /: Append[channel_WebSocketChannel, client: _SocketObject | _CSocket] := 
Module[{connections}, 
	connections = channel["Connections"]; 
	connections["Insert", client]; 

	Echo[client, "Added client:"];
	Echo[connections//Normal, "Current subscriptions:"];

	(*Return: WebSocketChannel[]*)
	channel
]; 


WebSocketChannel /: Delete[channel_WebSocketChannel, client: _SocketObject | _CSocket] := 
Module[{connections}, 
	connections = channel["Connections"]; 
	connections["Delete", client]; 

	Echo[client, "Deleted client:"];
	Echo[connections//Normal, "Current subscriptions:"];

	(*Return: WebSocketChannel[]*)
	channel
]; 


WebSocketChannel /: WebSocketSend[channel_WebSocketChannel, client: _SocketObject | _CSocket, message: _String | _ByteArray] := 
If[FailureQ[#], Delete[channel, client]]& @ WebSocketSend[client, message]; 


WebSocketChannel /: WebSocketSend[channel_WebSocketChannel, client: _SocketObject | _CSocket, expr_] := 
Module[{serializer, message}, 
	serializer = channel["Serializer"]; 
	message = serializer[expr]; 
	WebSocketSend[channel, client, message]; 
]; 


WebSocketChannel /: WebSocketSend[channel_WebSocketChannel, expr_] := 
Module[{connections}, 
	connections = channel["Connections"]; 
	Map[WebSocketSend[channel, #, expr]&, connections["Elements"]]; 
]; 


CreateType[WebSocketHandler, init, {
	"MessageHandler" -> <||>, 
	"DefaultMessageHandler" -> $defaultMessageHandler, 	
	"Deserializer" -> $deserializer, (*Input: <|.., "Data" -> ByteArray[]|>*)
	"Serializer" -> $serializer, (*Return: ByteArray[]*) 
	"Connections", 
	"Buffer"
}]; 


handler_WebSocketHandler[client: _SocketObject | _CSocket, message_ByteArray] := 
Module[{connections, deserializer, messageHandler, defaultMessageHandler, frame, buffer, data, expr}, 
	$CurrenctClient = client; 

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


WebSocketHandler /: WebSocketSend[handler_WebSocketHandler, client: _SocketObject | _CSocket, message_] := 
WebSocketSend[client, encodeFrame[handler, message]]; 


WebSocketHandler /: WebSocketChannel[handler_WebSocketHandler, name_String, clients: {___SocketObject | ___CSocket}: {}] := 
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


$deserializer = #&; 


$serializer = ExportByteArray[#, "Text"]&; 


$directory = DirectoryName[$InputFileName, 2]; 


$connections = <||>; 


WebSocketHandler /: init[handler_WebSocketHandler] := (
	handler["Connections"] = CreateDataStructure["HashSet"]; 
	handler["Buffer"] = CreateDataStructure["HashTable"]; 
);


WebSocketChannel /: init[channel_WebSocketChannel] := 
channel["Connections"] = CreateDataStructure["HashSet"]; 


getConnetionsByClient[client: _SocketObject | _CSocket] := 
(*Return: DataStructure[HashSet]*)
$connections[client]; 


handshakeQ[client: _SocketObject | _CSocket, message_ByteArray] := 
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


frameQ[client: _SocketObject | _CSocket, message_ByteArray] := 
Module[{connections}, 
	(*Result: DataStructure[HashSet]*)
	connections = getConnetionsByClient[client];

	(*Return: True | False*)
	DataStructureQ[connections] && connections["MemberQ", client]
]; 


closeQ[client: _SocketObject | _CSocket, message_ByteArray] := 
Module[{connections}, 
	(*Result: DataStructure[HashSet]*)
	connections = getConnetionsByClient[client]; 

	(*Return: True | False*)
	connections["MemberQ", client] && 
	FromDigits[IntegerDigits[message[[1]], 2, 8][[2 ;; ]], 2] == 8
]; 


pingQ[client: _SocketObject | _CSocket, message_ByteArray] := 
Module[{connections}, 
	(*Result: DataStructure[HashSet]*)
	connections = getConnetionsByClient[client]; 

	connections["MemberQ", client] && 
	FromDigits[IntegerDigits[message[[1]], 2, 8][[2 ;; ]], 2] == 9
]; 


pong[client: _SocketObject | _CSocket, message_ByteArray] := 
Module[{firstByte}, 
	firstByte = IntegerDigits[message[[1]], 2, 8]; 
	firstByte[[5 ;; 8]] = {1, 0, 1, 0}; 

	(*Return: ByteArray*)
	Join[ByteArray[{FromDigits[firstByte, 2]}], message[[2 ;; ]]]
]; 


handshake[client: _SocketObject | _CSocket, message_ByteArray] := 
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
Module[{byte1, fin, opcode, length, mask, lengthBytes, reserved}, 
	fin = {1}; 
	
	reserved = {0, 0, 0}; 

	opcode = IntegerDigits[1, 2, 4]; 

	byte1 = ByteArray[{FromDigits[Join[fin, reserved, opcode], 2]}]; 

	length = Length[message]; 

	Which[
		length < 126, 
			lengthBytes = ByteArray[{length}], 
		126 <= length < 2^16, 
			lengthBytes = ByteArray[Join[{126}, IntegerDigits[length, 256, 2]]], 
		2^16 <= length < 2^64, 
			lengthBytes = ByteArray[Join[{127}, IntegerDigits[length, 256, 8]]]
	]; 

	(*Return: _ByteArray*)
	ByteArray[Join[byte1, lengthBytes, message]]
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


getFrameLength[client: _SocketObject | _CSocket, message_ByteArray] := 
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

VersionQ[n_] := $VersionNumber >= n

unmask := unmask = 
If[VersionQ[13.2],
	PreCompile[{$directory, "unmask"}, FileNameJoin[{$directory, "Kernel", "unmask.wl"}]]
,
	FileNameJoin[{$directory, "Kernel", "unmask-uncompiled.wl"}] // Get
];

saveFrameToBuffer[buffer_DataStructure, client: (SocketObject | CSocket)[uuid_], frame_] := 
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


getDataAndDropBuffer[buffer_DataStructure, client: (SocketObject | CSocket)[uuid_], frame_] := 
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
