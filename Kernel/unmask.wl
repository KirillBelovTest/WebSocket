		FunctionCompile[Function[{
			Typed[maskingKey, "NumericArray"::["MachineInteger", 1]], 
			Typed[payload, "NumericArray"::["MachineInteger", 1]]
		}, 
			(*Return: PacketArray::[MachineInteger, 1]*)
			Table[BitXor[payload[[i]], maskingKey[[Mod[i - 1, 4] + 1]]], {i, 1, Length[payload]}]
		]]