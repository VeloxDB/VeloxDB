using System;
using System.IO;
using System.Net.Security;
using System.Net.Sockets;
using VeloxDB.Common;

namespace VeloxDB.Networking;

internal sealed class ServerConnection : Connection
{
	Host host;

	public ServerConnection(Host host, Socket socket, Stream stream, MessageChunkPool chunkPool, TimeSpan inactivityInterval,
		TimeSpan inactivityTimeout, int maxQueuedChunkCount, bool groupSmallMessages, HandleMessageDelegate messageHandler,
		JobWorkers<Action> priorityWorkers = null) :
		base(socket, stream, chunkPool, inactivityInterval, inactivityTimeout, maxQueuedChunkCount, groupSmallMessages,
		     messageHandler, priorityWorkers)
	{
		this.host = host;

		if (inactivityTimeout != TimeSpan.MaxValue)
			NativeSocket.TurnOnKeepAlive(socket.Handle, inactivityInterval, inactivityTimeout);

		socket.NoDelay = false;
		socket.ReceiveBufferSize = tcpBufferSize;
		socket.SendBufferSize = tcpBufferSize;

		// Start receiving immediately since we are connected
		StartReceiving();
	}

	public override ulong MessageIdBit => 0x8000000000000000;

	protected override bool IsResponseMessage(ulong msgId)
	{
		return (msgId & 0x8000000000000000) == 0x8000000000000000;
	}

	protected override void OnDisconnected()
	{
		host.ConnectionDisconnected(this);
	}
}
