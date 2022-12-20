using System;
using System.Collections.Generic;
using System.Text;

namespace VeloxDB.Client;

/// <summary>
/// Provides a simple way to build a connection string.
/// </summary>
public sealed class ConnectionStringParams
{
	const int defaultPoolSize = 4;
	const int defaultBufferPoolSize = 1024 * 1024;  // 1 MB
	const int defaultOpenTimeout = 5000;            // 5 sec
	const int defaultRetryTimeout = 5000;
	const string defaultServiceName = "";

	const string poolSizeParam = "pool_size";
	const string buffPoolSizeParam = "buff_pool_size";
	const string addressParam = "address";
	const string openTimeoutParam = "open_timeout";
	const string serviceNameParam = "service_name";
	const string retryTimeoutParam = "retry_timeout";

	const string invalidConnStrErr = "Invalid connection string.";

	List<string> addresses = new List<string>();
	int poolSize;
	int bufferPoolSize;
	int openTimeout;
	string serviceName;
	int retryTimeout;

	/// <summary>
	/// Creates an empty instance of ConnectionStringParams.
	/// </summary>
	public ConnectionStringParams()
	{
		addresses = new List<string>(2);
		poolSize = defaultPoolSize;
		bufferPoolSize = defaultBufferPoolSize;
		openTimeout = defaultOpenTimeout;
		serviceName = defaultServiceName;
		retryTimeout = defaultRetryTimeout;
	}

	internal ConnectionStringParams(string connectionString) :
		this()
	{
		connectionString = connectionString.Trim();

		string[] s = connectionString.Split(";");
		for (int i = 0; i < s.Length; i++)
		{
			if (s[i].Length > 0)
				SetParamValue(s[i]);
		}
	}

	/// <summary>
	/// Gets addresses, to add an address use <see cref="AddAddress(string)"/>.
	/// </summary>
	public string[] Addresses => addresses.ToArray();

	/// <summary>
	/// Specifies number of connections in connection pool.
	/// </summary>
	public int PoolSize { get => poolSize; set => poolSize = value; }

	/// <summary>
	/// Gets or sets connection's buffer size in bytes.
	/// </summary>
	/// <remarks>
	///  This buffer pool is used for serializing and deserializing messages. If your messages are larger than this number
	///  communication layer will often need to allocate memory, which might have performance impact.
	/// </remarks>
	public int BufferPoolSize { get => bufferPoolSize; set => bufferPoolSize = value; }

	/// <summary>
	/// Gets or sets how much to wait for connection to open in milliseconds.
	/// </summary>
	public int OpenTimeout { get => openTimeout; set => openTimeout = value; }

	/// <summary>
	/// Gets or sets name of service to which to connect. For database apis, this should be empty string. Default value is empty string.
	/// </summary>
	public string ServiceName { get => serviceName; set => serviceName = value; }

	/// <summary>
	/// Gets or sets the time span (in milliseconds) during which the request will be auto-retried
	/// before giving up and throwing an exception.
	/// </summary>
	public int RetryTimeout { get => retryTimeout; set => retryTimeout = value; }

	/// <summary>
	/// Add address to which to connect. Multiple addressess can be added. In case of multiple addresses each is tried until connection succeeds.
	/// </summary>
	public void AddAddress(string address)
	{
		addresses.Add(address);
	}

	/// <summary>
	/// Create connection string. This string should be passed to <see cref="VeloxDB.Client.ConnectionFactory.Get">ConnectionFactory.Get</see>.
	/// </summary>
	public string GenerateConnectionString()
	{
		StringBuilder sb = new StringBuilder();
		AppendAddresses(sb);

		if (poolSize != defaultPoolSize)
			sb.AppendFormat($"{poolSizeParam}={poolSize}; ");

		if (bufferPoolSize != defaultBufferPoolSize)
			sb.AppendFormat($"{buffPoolSizeParam}={bufferPoolSize}; ");

		if (openTimeout != defaultOpenTimeout)
			sb.AppendFormat($"{openTimeoutParam}={openTimeout}; ");

		if (retryTimeout != defaultRetryTimeout)
			sb.AppendFormat($"{retryTimeoutParam}={retryTimeout}; ");

		if (!serviceName.Equals(defaultServiceName, StringComparison.Ordinal))
			sb.AppendFormat($"{serviceNameParam}={serviceName}; ");

		return sb.ToString();
	}

	private void AppendAddresses(StringBuilder sb)
	{
		for (int i = 0; i < addresses.Count; i++)
		{
			sb.AppendFormat($"{addressParam}={addresses[i]}; ");
		}
	}

	private void SetParamValue(string v)
	{
		string[] s = v.Split("=");
		if (s.Length != 2)
			throw new ArgumentException(invalidConnStrErr);

		string key = s[0].Trim().ToLower();
		if (key.Equals(addressParam, StringComparison.Ordinal))
		{
			addresses.Add(s[1].Trim());
		}
		else if (key.Equals(poolSizeParam, StringComparison.Ordinal))
		{
			if (!int.TryParse(s[1].Trim().ToLower(), out poolSize))
				throw new ArgumentException(invalidConnStrErr);
		}
		else if (key.Equals(buffPoolSizeParam, StringComparison.Ordinal))
		{
			if (!int.TryParse(s[1].Trim().ToLower(), out bufferPoolSize))
				throw new ArgumentException(invalidConnStrErr);
		}
		else if (key.Equals(openTimeoutParam, StringComparison.Ordinal))
		{
			if (!int.TryParse(s[1].Trim().ToLower(), out openTimeout))
				throw new ArgumentException(invalidConnStrErr);
		}
		else if (key.Equals(retryTimeoutParam, StringComparison.Ordinal))
		{
			if (!int.TryParse(s[1].Trim().ToLower(), out retryTimeout))
				throw new ArgumentException(invalidConnStrErr);
		}
		else if (key.Equals(serviceNameParam, StringComparison.Ordinal))
		{
			serviceName = s[1];
		}
		else
		{
			throw new ArgumentException(invalidConnStrErr);
		}
	}
}
