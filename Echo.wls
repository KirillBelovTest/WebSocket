#!/usr/bin/env wolframscript
(* ::Package:: *)

Get["KirillBelov`WebSocket`"]; 


ws = WebSocketServerCreate[8080, <|
	"Echo" -> Function[True] -> Function[Echo[#2]]
|>]; 


ws["Listener"]


conn = WebSocketConnect["ws://localhost:8080"];


WebSocketSend[conn, "hello"]


Close[ws["Listener"]]; 
