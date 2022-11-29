using System;
using System.Net.Sockets;

namespace Velox.Networking;

internal sealed class ServerConnection : Connection
{
	Host host;

	public ServerConnection(Host host, Socket socket, int bufferPoolSize, TimeSpan inactivityInterval,
		TimeSpan inactivityTimeout, int maxQueuedChunkCount, bool groupSmallMessages, HandleMessageDelegate messageHandler) :
		base(socket, bufferPoolSize, inactivityInterval, inactivityTimeout, maxQueuedChunkCount, groupSmallMessages, messageHandler)
	{
		this.host = host;

		if (inactivityTimeout != TimeSpan.MaxValue)
			NativeSocket.TurnOnKeepAlive(socket.Handle, inactivityInterval, inactivityTimeout);

		socket.NoDelay = true;
		socket.ReceiveBufferSize = MessageChunk.LargeBufferSize * 2;
		socket.SendBufferSize = MessageChunk.LargeBufferSize * 2;

		// Start receiving immediately since we are connected
		StartReceivingAsync();
	}

	protected override long BaseMsgId => long.MinValue;

	protected override bool IsResponseMessage(long msgId)
	{
		return msgId < 0;
	}

	protected override void OnDisconnected()
	{
		host.ConnectionDisconnected(this);
	}
}
