		Function[{
			maskingKey,
			payload
		}, 
			(*Return: PacketArray::[MachineInteger, 1]*)
			Table[BitXor[payload[[i]], maskingKey[[Mod[i - 1, 4] + 1]]], {i, 1, Length[payload]}]
		]