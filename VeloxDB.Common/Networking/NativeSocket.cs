using System;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Security;
using VeloxDB.Common;
using System.Runtime.Versioning;

namespace VeloxDB.Networking;

internal static class NativeSocket
{
	private delegate int SetSockOpt(IntPtr socket, int level, int option_name, ref int option_value, uint option_len);
	readonly static SetSockOpt setsockopt;

	// TCP_KEEPALIVE - idle time in between keep-alives when there is a response from the peer
	// TCP_KEEPINTVL - interval between keep-alives when there is no response from the peer,
	//				   this is done to probe the peer until there is a response.
	// TCP_KEEPCNT - number of times keep-alives are repeated before a close when there is no response
	static readonly int SOL_SOCKET = -1;        // options for socket level
	static readonly int SO_KEEPALIVE = -1;      // keep connections alive
	static readonly int IPPROTO_TCP = -1;       // TCP
	static readonly int TCP_KEEPALIVE = -1;     // idle time used when SO_KEEPALIVE is enabled
	static readonly int TCP_KEEPINTVL = -1;     //interval between keepalives
	static readonly int TCP_KEEPCNT = -1;       // number of keepalives before close					

	static NativeSocket()
	{
		if (OperatingSystem.IsLinux())
		{
			SOL_SOCKET = 1;
			SO_KEEPALIVE = 9;
			IPPROTO_TCP = 6;
			TCP_KEEPALIVE = 4;
			TCP_KEEPINTVL = 5;
			TCP_KEEPCNT = 6;
			setsockopt = LinuxSetsockopt;
		}
		else if (OperatingSystem.IsMacOS())
		{
			SOL_SOCKET = 0xffff;
			SO_KEEPALIVE = 0x0008;
			IPPROTO_TCP = 6;
			TCP_KEEPALIVE = 0x10;
			TCP_KEEPINTVL = 0x101;
			TCP_KEEPCNT = 0x102;
			setsockopt = OSXSetsockopt;
		}
	}

	public static bool IsAddressAlreadyInUseError(int errorCode)
	{
		if (OperatingSystem.IsWindows())
		{
			const int WSAEADDRINUSE = 10048;
			const int WSAEACCES = 10013;
			return (errorCode == WSAEADDRINUSE || errorCode == WSAEACCES);
		}
		if (OperatingSystem.IsMacOS())
		{
			const int EADDRINUSE = 48;
			return (errorCode == EADDRINUSE);
		}
		if (OperatingSystem.IsLinux())
		{
			const int EADDRINUSE = 98;
			return (errorCode == EADDRINUSE);
		}

		throw Utils.OSNotSupportedException();
	}

	public unsafe static void TurnOnKeepAlive(IntPtr socket, TimeSpan inactivityInterval, TimeSpan timeout)
	{
		if (OperatingSystem.IsWindows())
		{		
			TurnOnKeepAliveWindows(socket, inactivityInterval, timeout);
			return;
		}
		if (OperatingSystem.IsLinux() || OperatingSystem.IsMacOS())
		{
			TurnOnKeepAlivePOSIX(socket, inactivityInterval, timeout);
			return;
		}

		throw Utils.OSNotSupportedException();
	}

	[DllImport("libc", EntryPoint = "setsockopt", SetLastError = true)]
	[SuppressUnmanagedCodeSecurity]
	private static extern int LinuxSetsockopt(IntPtr socket, int level, int option_name, ref int option_value, uint option_len);

	[DllImport("libSystem.dylib", EntryPoint = "setsockopt", SetLastError = true)]
	[SuppressUnmanagedCodeSecurity]
	private static extern int OSXSetsockopt(IntPtr socket, int level, int option_name, ref int option_value, uint option_len);

	[SupportedOSPlatform("macos")]
	[SupportedOSPlatform("linux")]
	private static void TurnOnKeepAlivePOSIX(IntPtr socket, TimeSpan inactivityInterval, TimeSpan timeout)
	{
		int retransmitTimeout = 1;  // 1s
		int on = 1;

		int idleTime = (int)Math.Min((long)inactivityInterval.TotalSeconds, int.MaxValue);
		int timeoutS = (int)Math.Min((long)timeout.TotalSeconds, int.MaxValue);
		idleTime = Math.Max(idleTime, 1);
		timeoutS = Math.Max(timeoutS, 1);
		int retransmitCount = timeoutS / retransmitTimeout;

		if (setsockopt(socket, SOL_SOCKET, SO_KEEPALIVE, ref on, sizeof(int)) == -1)
			throw new SocketException(Marshal.GetLastWin32Error());
		if (setsockopt(socket, IPPROTO_TCP, TCP_KEEPALIVE, ref idleTime, sizeof(int)) == -1)
			throw new SocketException(Marshal.GetLastWin32Error());
		if (setsockopt(socket, IPPROTO_TCP, TCP_KEEPINTVL, ref retransmitTimeout, sizeof(int)) == -1)
			throw new SocketException(Marshal.GetLastWin32Error());
		if (setsockopt(socket, IPPROTO_TCP, TCP_KEEPCNT, ref retransmitCount, sizeof(int)) == -1)
			throw new SocketException(Marshal.GetLastWin32Error());
	}

	[SupportedOSPlatform("windows")]
	private unsafe static void TurnOnKeepAliveWindows(IntPtr socket, TimeSpan inactivityInterval, TimeSpan timeout)
	{
		const int windowsRetransmitCount = 10;  // This is hard coded in windows :(

		int intervalMS = (int)Math.Min((long)inactivityInterval.TotalMilliseconds, int.MaxValue);
		int timeoutMS = (int)Math.Min((long)timeout.TotalMilliseconds, int.MaxValue);

		int retransmitTimeout = Math.Max(timeoutMS / windowsRetransmitCount, 200);

		tcp_keepalive tk = new tcp_keepalive() { onoff = 1, keepalivetime = (uint)intervalMS,
			keepaliveinterval = (uint)retransmitTimeout };

		int bytesReturned = 0;
		if (WSAIoctl(socket, IOControlCode.KeepAliveValues, (IntPtr)(&tk), sizeof(tcp_keepalive),
			IntPtr.Zero, 0, ref bytesReturned, IntPtr.Zero, IntPtr.Zero) != 0)
		{
			throw new SocketException(WSAGetLastError());
		}
	}

	[DllImport("Ws2_32.dll")]
	[SuppressUnmanagedCodeSecurity]
	private static extern int WSAIoctl(IntPtr s, System.Net.Sockets.IOControlCode dwIoControlCode, IntPtr lpvInBuffer, int cbInBuffer,
		IntPtr lpvOutBuffer, int cbOutBuffer, ref int lpcbBytesReturned, IntPtr lpOverlapped, IntPtr lpCompletionRoutine);

	[DllImport("ws2_32.dll", CharSet = CharSet.Auto, SetLastError = true)]
	[SuppressUnmanagedCodeSecurity]
	public static extern int WSAGetLastError();

	[StructLayout(LayoutKind.Sequential)]
	private struct tcp_keepalive
	{
		public uint onoff;
		public uint keepalivetime;
		public uint keepaliveinterval;
	}
}
