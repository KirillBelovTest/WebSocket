#!/usr/bin/env wolframscript
(* ::Package:: *)

<<KirillBelov`Internal`
<<KirillBelov`TCPServer`
<<KirillBelov`WebSocketHandler`

tcp = TCPServer[]

tcp["CompleteHandler", "WebSocket"] = WebSocketPacketQ -> WebSocketPacketLength
tcp["MessageHandler", "WebSocket"]  = WebSocketPacketQ -> ws

ws = WebSocketHandler[]
ws["MessageHandler", "Evaluate"]  = Function[True] -> evaluate

kernel = LaunchKernels[1][[1]]

evaluateAndSend[client_SocketObject, data_ByteArray] := 
Module[{code, result, ejson}, 
	code = ByteArrayToString[data]; 
	result = ToExpression[code]; 
	ejson = ExportByteArray[result, "ExpressionJSON"]; 
	WebSocketSend[client, ejson]; 
]


DistributeDefinitions[evaluateAndSend]


ParallelEvaluate[Language`ExtendedFullDefinition[] = #]& @ Language`ExtendedFullDefinition[evaluateAndSend]


evaluate = Function[
	evaluateAndSend[#1, #2]
]

listener = SocketListen[8000, tcp@#&]
While[True, Pause[0.01]]
