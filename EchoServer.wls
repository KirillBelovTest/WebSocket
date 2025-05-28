#!/usr/bin/env wolframscript

PacletDirectoryLoad[Directory[]];


Get["KirillBelov`WebSocket`"];


server = CSocketOpen[8080];


handler = CSocketHandler[]; 


listener = SocketListen[server, (Echo[#]; handler[#])&]; 


ws = WebSocketHandler[]; 


ws["Deserializer"] = ByteArrayToString;


ws["DefaultMessageHandler"] = Function[Echo[#2, "MSG:"];]


handler["Accumulator", "WS"] = WebSocketPacketQ -> WebSocketPacketLength; 


handler["Handler", "WS"] = WebSocketPacketQ -> ws; 


While[True, Pause[1]];
