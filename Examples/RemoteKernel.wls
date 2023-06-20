#!/usr/bin/env wolframscript
(* ::Package:: *)

(*Init*)


<<KirillBelov`Internal`
<<KirillBelov`TCPServer`
<<KirillBelov`WebSocketHandler`


(*TCP*)


tcp = TCPServer[]


tcp["CompleteHandler", "WebSocket"] = WebSocketPacketQ -> WebSocketPacketLength
tcp["MessageHandler", "WebSocket"]  = WebSocketPacketQ -> ws


(*WS*)


ws = WebSocketHandler[]


ws["MessageHandler", "Evaluate"]  = Function[True] -> evaluate


evaluate = Function[
	WebSocketSend[
		#1, 
		ExportByteArray[
			Echo[
				ToExpression[ByteArrayToString[#2]], 
				"Result"
			], 
			"ExpressionJSON"
		]
	]
]


(*Start*)


listener = SocketListen[8000, tcp@#&]


While[True, Pause[0.01]]
