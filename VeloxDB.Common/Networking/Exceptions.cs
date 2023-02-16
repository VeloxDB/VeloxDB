using System;

namespace VeloxDB.Networking;

/// <summary>
/// Base communication exception.
/// </summary>
public class CommunicationException : Exception
{
	///
	public CommunicationException() : this("Communication error occurred.", null) { }
	///
	public CommunicationException(Exception inner) : this("Communication error occurred.", inner) { }
	///
	public CommunicationException(string message, Exception inner) : base(message, inner) { }
}

internal class AddressAlreadyInUseException : CommunicationException
{
	public AddressAlreadyInUseException() : this("Address already in use.", null) { }
	public AddressAlreadyInUseException(string message, Exception inner) : base(message, inner) { }
}

/// <summary>
/// Gives an information whether the connection has been aborted during opening phase or during communication phase.
/// </summary>
public enum AbortedPhase
{
	/// <summary>
	/// Connection has been aborted during an attempt to perform a connect operation.
	/// </summary>
	OpenAttempt = 1,

	/// <summary>
	/// Connection has been aborted during communication, after already being connected.
	/// </summary>
	Communication = 2
}

/// <summary>
/// Exception that is thrown when connection is closed.
/// </summary>
public class CommunicationObjectAbortedException : CommunicationException
{
	AbortedPhase abortedPhase;

	///
	public CommunicationObjectAbortedException(AbortedPhase abortedPhase) : this(abortedPhase, "Communication object has been aborted.", null) { }
	///
	public CommunicationObjectAbortedException(AbortedPhase abortedPhase, string message, Exception inner) : base(message, inner)
	{
		this.abortedPhase = abortedPhase;
	}

/// <summary>
/// Indicates at what phase has connection been aborted.
/// </summary>
public AbortedPhase AbortedPhase => abortedPhase;
}

internal class ConnectionNotOpenException : CommunicationException
{
	public ConnectionNotOpenException() : this("Connection has not been open.", null) { }
	public ConnectionNotOpenException(string message, Exception inner) : base(message, inner) {	}
}

internal class ChunkTimeoutException : CommunicationException
{
	public ChunkTimeoutException() : this("Waiting for the next chunk exceeded the timeout.", null) { }
	public ChunkTimeoutException(string message, Exception inner) : base(message, inner) { }
}

internal class CorruptMessageException : CommunicationException
{
	public CorruptMessageException() :
		this("Message data could not be read because it has either been corrupted or it is in unknown format.", null) { }
	public CorruptMessageException(string message, Exception inner) : base(message, inner) { }
}

/// <summary>
/// Exception thrown when trying to connect to server using newer protocol version.
/// </summary>
public class UnsupportedHeaderException : Exception
{
	///
	public UnsupportedHeaderException() : this("Message format is unsupported.") { }
	///
	public UnsupportedHeaderException(string message) : base(message) { }
	///
	public UnsupportedHeaderException(string message, Exception inner) : base(message, inner) { }
}
