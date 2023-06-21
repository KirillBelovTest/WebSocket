(* ::Package:: *)

(* ::Chapter:: *)
(*WS Handler*)


(* ::Program:: *)
(*+-------------------------------------------------+*)
(*|              WEBSSOCKET HANDLER                 |*)
(*|                                                 |*)
(*|               (reseive message)                 |*)
(*|                       |                         |*)
(*|              <check message type>               |*)
(*|             /         |          \              |*)
(*|  [handshake]       [frame]        [close frame] |*)
(*|      |                |                 |       |*)
(*| [send accept]     [decode]           {close}    |*)
(*|      |                |                         |*)
(*|   {to tcp}      [deserialize]                   |*)
(*|                       |                         |*)
(*|                <select pipeline>                |*)
(*|      /        /                 \       \       |*)
(*|    ..    [callback]         [subscribe]  ..     |*)
(*|                  \            /                 |*)
(*|                   <check type>                  |*)
(*|             null /            \ data            |*)
(*|            {next}            [serialize]        |*)
(*|                                   |             |*)
(*|                                {to tcp}         |*)
(*+-------------------------------------------------+*)


(*::Section::Close::*)
(*Begin package*)


BeginPackage["KirillBelov`WebSocketHandler`", {"KirillBelov`Internal`", "KirillBelov`Objects`"}]; 


(*::Section::Close::*)
(*Names*)


ClearAll["`*"]; 


WebSocketPacketQ::usage = 
"WebSocketPacketQ[client, packet] check that packet sent via WebSocket protocol."; 


WebSocketPacketLength::usage = 
"WSLength[client, message] get expected message length."; 


WebSocketSend::uasge = 
"WebSocketSend[client, message] send message via WebSocket protocol."; 


WebSocketHandler::usage = 
"WebSocketHandler[opts] handle messages received via WebSocket protocol."; 


(*::Section::Close::*)
(*Begin private*)


Begin["`Private`"]; 


WebSocketPacketQ[client_SocketObject, message_ByteArray] := 
frameQ[client, message] || handshakeQ[client, message]; 


WebSocketPacketLength[client_SocketObject, message_ByteArray] := 
If[frameQ[client, message], 
	getFrameLength[client, message], 
	Length[message]
]; 


WebSocketSend[client_SocketObject, message: _String | _ByteArray] := 
Switch[message, 
	_String, WriteString[client, encodeFrame[message]];, 
	_ByteArray, BinaryWrite[client, encodeFrame[message]];
]; 


WebSocketHandler /: WebSocketSend[handler_WebSocketHandler, client_SocketObject, message_] := 
WebSocketSend[client, encodeFrame[handler, message]]; 


CreateType[WebSocketHandler, {
	"MessageHandler" -> <||>, 
	"DefaultMessageHandler" -> $defaultMessageHandler, 	
	"Deserializer" -> $deserializer, (*Input: <|.., "Data" -> ByteArray[]|>*)
	"Serializer" -> $serializer, (*Return: ByteArray[]*)
	"Connections" -> {}
}]; 


handler_WebSocketHandler[client_SocketObject, message_ByteArray] := 
Module[{deserializer, serializer, messageHandler, defaultMessageHandler, frame, data, result}, 
	deserializer = handler["Deserializer"]; 
	serializer = handler["Serializer"]; 

	Which[
		(*Return: Null*)
		closeQ[client, message], 
			Close[client]; 
			DeleteCases[$connections, client]; , 

		(*Return: Null*)
		frameQ[client, message], 
			frame = decodeFrame[message, deserializer]; 
			data = frame["Data"]; 

			messageHandler = handler["MessageHandler"]; 
			defaultMessageHandler = handler["DefaultMessageHandler"]; 
			
			ConditionApply[messageHandler, defaultMessageHandler][client, data];, 

		(*Return: _String*)
		handshakeQ[client, message], 
			handshake[client, message]
	]
]; 


(*::Section::Close::*)
(*Internal*)


$guid = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"; 


$connections = {}; 


$httpEndOfHead = StringToByteArray["\r\n\r\n"]; 


$defaultMessageHandler = Close@#&; 


$deserializer = #&; 


$serializer = ExportByteArray[#, "Text"]&; 


$directory = DirectoryName[$InputFileName, 2]; 


handshakeQ[client_SocketObject, message_ByteArray] := 
Module[{head}, 
	(*Return: True | False*)
	If[frameQ[client, message], 
		False, 
	(*Else*)
		head = ByteArrayToString[BytesSplit[message, $httpEndOfHead -> 1][[1]]]; 

		Length[message] != StringLength[head] && 
		StringContainsQ[head, StartOfString ~~ "GET /"] && 
		StringContainsQ[head, StartOfLine ~~ "Upgrade: websocket"]
	]
]; 


frameQ[client_SocketObject, message_ByteArray] := 
(*Return: True | False*)
MemberQ[$connections, client]; 


closeQ[client_SocketObject, message_ByteArray] := 
(*Return: True | False*)
FromDigits[IntegerDigits[message[[1]], 2, 8][[2 ;; ]], 2] == 8; 


handshake[client_SocketObject, message_ByteArray] := 
Module[{messageString, key, acceptKey}, 
	messageString = ByteArrayToString[message]; 
	$connections = DeleteDuplicates[Append[$connections, client]]; 
	
	key = StringExtract[messageString, "Sec-WebSocket-Key: " -> 2, "\r\n" -> 1]; 
	acceptKey = createAcceptKey[key]; 

	(*Return: _String*)
	"HTTP/1.1 101 Switching Protocols\r\n" <> 
	"Connection: upgrade\r\n" <> 
	"Upgrade: websocket\r\n" <> 
	"Sec-WebSocket-Accept: " <> acceptKey <> "\r\n\r\n"
]; 


createAcceptKey[key_String] := 
(*Return: _String*)
BaseEncode[Hash[key <> $guid, "SHA1", "ByteArray"], "Base64"]; 


getFrameLength[client_SocketObject, message_ByteArray] := 
Module[{length}, 
	length = FromDigits[IntegerDigits[message[[2]], 2, 8][[2 ;; ]], 2]; 

	Which[
		length == 126, length = FromDigits[Normal[message[[3 ;; 4]]], 256], 
		length == 127, length = FromDigits[Normal[message[[3 ;; 10]]], 256]
	]; 

	(*Return: _Integer*)
	length
]; 


encodeFrame[message_ByteArray] := 
Module[{byte1, fin, opcode, length, mask, lengthBytes}, 
	fin = {1}; 
	
	received = {0, 0, 0}; 

	opcode = IntegerDigits[1, 2, 4]; 

	byte1 = ByteArray[{FromDigits[Join[fin, received, opcode], 2]}]; 

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


decodeFrame[message_ByteArray, deserializer_] := 
Module[{header, payload, data}, 
	header = getFrameHeader[message]; 
	payload = message[[header["PayloadPosition"]]]; 
	data = If[Length[header["MaskingKey"]] == 4, ByteArray[unmask[header["MaskingKey"], payload]], payload]; 

	(*Return: _Association*)
	Append[header, "Data" -> deserializer[data]]
]; 


getFrameHeader[message_ByteArray] := 
Module[{byte1, byte2, fin, opcode, mask, len, maskingKey, nextPosition, payload, data}, 
	byte1 = IntegerDigits[message[[1]], 2, 8]; 
	byte2 = IntegerDigits[message[[2]], 2, 8]; 

	fin = byte1[[1]] == 1; 
	opcode = Switch[FromDigits[byte1[[2 ;; ]], 2], 
		1, "Part", 
		2, "Text", 
		4, "Binary", 
		8, "Close"
	]; 

	mask = byte2[[1]] == 1; 

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


unmask := unmask = PreCompile[{$directory, "unmask"}, 
	FunctionCompile[Function[{
		Typed[maskingKey, "NumericArray"::["MachineInteger", 1]], 
		Typed[payload, "NumericArray"::["MachineInteger", 1]]
	}, 
		(*Return: PacketArray::[MachineInteger, 1]*)
		Table[BitXor[payload[[i]], maskingKey[[Mod[i - 1, 4] + 1]]], {i, 1, Length[payload]}]
	]]
]; 


(*::Section::Close::*)
(*End private*)


End[]; 


(*::Section::Close::*)
(*End package*)


EndPackage[]; 
