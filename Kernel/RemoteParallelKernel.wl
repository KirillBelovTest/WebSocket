(* ::Package:: *)

BeginPackage["KirillBelov`WebSocketHandler`RemoteParallelKernel`", {
	"KirillBelov`TCPServer`", 
	"KirillBelov`WebSocketHandler`", 
	"KirillBelov`WebSocketHandler`Extensions`"
}]; 


RPKStart::usage = 
"RPKStart[port] starts remote parallel kernel via WebSocket protocol."; 


RPKStop::usage = 
"RPKStop[port] stops kernel."; 


RPKReadyQ::usage = 
"RPKReadyQ[port] check that kernel is ready."; 


Begin["`Private`"]; 


RPKStart[port_Integer] := 
Module[{link}, 
	link = LinkLaunch[First[$CommandLine] <> " -wstp"]; 
	LinkRead[link];
	With[{definition = Language`ExtendedFullDefinition[createRemoteWebSocketKernel]}, 
		LinkWrite[link, Unevaluated[Function[Language`ExtendedFullDefinition[] = #][definition]]];
		TimeConstrained[While[!LinkReadyQ[link], Pause[0.001]], 10]; 
		If[LinkReadyQ[link], LinkRead[link]]; 
		LinkWrite[link, Unevaluated[listener = createRemoteWebSocketKernel[port]]]; 
		LinkRead[link]; 

		(*Return: LinkObject*)
		$links[port] = link
	]
]; 


RPKStop[port_Integer] := 
If[KeyExistsQ[$links, port], Close[$links[port]; Delete[$links, Key[port]]]]; 


$links = <||>; 


remoteWebSocketKernelEvaluate[client_SocketObject, message_ByteArray] := 
WebSocketSend[client, ExportByteArray[ImportByteArray[message, "WL"], "ExpressionJSON"]]; 


createRemoteWebSocketKernel[port_Integer] := 
Module[{ws, tcp}, 
	ws = WebSocketHandler[]; 
	ws["MessageHandler", "RemoteKernel"] = Function[True] -> remoteWebSocketKernelEvaluate; 
	tcp = TCPServer[]; 
	AddWebSocketHandler[tcp, ws]; 

	(*Return: SocketListener[]*)
	With[{t = tcp}, SocketListen[port, t@#&]]
]; 

End[]; 


EndPackage[]; 
