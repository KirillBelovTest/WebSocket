#!/usr/bin/env wolframscript
(* ::Package:: *)

Get["KirillBelov`WebSocket`"]; 


ws = WebSocketServerCreate[8080, <|
	"Echo" -> Function[True] -> Function[Echo[#2]]
|>]; 
