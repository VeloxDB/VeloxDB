using System;

namespace Velox.Protocol;

internal enum RequestType : byte
{
	Connect = 1,
	Operation = 2
}

internal enum ResponseType : byte
{
	Response = 1,
	Error = 2,
	APIUnavailable = 3,
	ProtocolError = 4,
}
