using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Intrinsics.Arm;
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
	const int defaultRetryTimeout = 10000;
	const string defaultServiceName = "";
	const bool defaultUseSSL = false;
	const string defaultCACert = "";
	const bool defaultVerifyCert = true;

	const string poolSizeParam = "pool_size";
	const string buffPoolSizeParam = "buff_pool_size";
	const string addressParam = "address";
	const string openTimeoutParam = "open_timeout";
	const string serviceNameParam = "service_name";
	const string retryTimeoutParam = "retry_timeout";
	const string useSSLParam = "use_ssl";
	const string caCertParam = "ca_certificate";
	const string serverCertParam = "server_certificate";
	const string verifyCertParam = "verify_certificate";

	List<string> addresses = new List<string>();
	int poolSize;
	int bufferPoolSize;
	int openTimeout;
	string serviceName;
	int retryTimeout;
	bool useSSL;
	string caCert;
	List<string> serverCerts = new List<string>();
	bool verifyCert;

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
		useSSL = defaultUseSSL;
		caCert = defaultCACert;
		verifyCert = defaultVerifyCert;
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

		Validate();
	}

    /// <summary>
    /// Returns a clone of the current ConnectionStringParams object.
    /// Returns:
    ///  ConnectionStringParams: A new instance with the same attributes.
    /// </summary>
	public ConnectionStringParams Clone()
	{
		var clone = new ConnectionStringParams();
		clone.addresses = new List<string>(this.addresses);
		clone.poolSize = this.poolSize;
		clone.bufferPoolSize = this.bufferPoolSize;
		clone.openTimeout = this.openTimeout;
		clone.serviceName = this.serviceName;
		clone.retryTimeout = this.retryTimeout;
		clone.useSSL = this.useSSL;
		clone.caCert = this.caCert;
		clone.verifyCert = this.verifyCert;
		clone.serverCerts = new List<string>(this.serverCerts);

		return clone;
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
	/// Gets or sets a value indicating whether SSL/TLS encryption should be used for the request.
	/// </summary>
	public bool UseSSL { get => useSSL; set => useSSL = value; }

	/// <summary>
	/// Gets or sets the path to the Certificate Authority (CA) certificate file used for SSL/TLS verification.
	/// </summary>
	public string CACert { get => caCert; set => caCert = value; }

	/// <summary>
	/// Gets or sets the path to the server certificate file used for SSL/TLS verification. To add server certificates
	/// see <see cref="AddServerCert(string)"/>
	/// </summary>
	public string[] ServerCerts => serverCerts.ToArray();

	/// <summary>
	/// Gets or sets a value indicating whether SSL/TLS certificate verification should be disabled.
	/// </summary>
	public bool VerifyCert { get => verifyCert; set => verifyCert = value; }

	/// <summary>
	/// Add address to which to connect. Multiple addresses can be added. In case of multiple addresses each is tried until connection succeeds.
	/// </summary>
	public void AddAddress(string address)
	{
		addresses.Add(address);
	}

	/// <summary>
	/// Add server certificate to verify server identity. Multiple certificates can be added (one for each connection added).
	/// </summary>
	public void AddServerCert(string serverCert)
	{
		serverCerts.Add(serverCert);
	}

	/// <summary>
	/// Create connection string. This string should be passed to <see cref="VeloxDB.Client.ConnectionFactory.Get(string)">ConnectionFactory.Get</see>.
	/// </summary>
	/// <exception cref="InvalidConnectionStringException">If resulting connection string is invalid</exception>
	public string GenerateConnectionString()
	{
		Validate();
		StringBuilder sb = new StringBuilder();
		AppendAddresses(sb);
		AppendCerts(sb);

		if (poolSize != defaultPoolSize)
			sb.AppendFormat($"{poolSizeParam}={poolSize}; ");

		if (bufferPoolSize != defaultBufferPoolSize)
			sb.AppendFormat($"{buffPoolSizeParam}={bufferPoolSize}; ");

		if (openTimeout != defaultOpenTimeout)
			sb.AppendFormat($"{openTimeoutParam}={openTimeout}; ");

		if (retryTimeout != defaultRetryTimeout)
			sb.AppendFormat($"{retryTimeoutParam}={retryTimeout}; ");

		if (serviceName != null && !serviceName.Equals(defaultServiceName, StringComparison.Ordinal))
			sb.AppendFormat($"{serviceNameParam}={serviceName}; ");

		if (useSSL != defaultUseSSL)
			sb.Append($"{useSSLParam}={useSSL}; ");

		if (caCert != null && !caCert.Equals(defaultCACert))
			sb.Append($"{caCertParam}={caCert}; ");

		if (verifyCert != defaultVerifyCert)
			sb.Append($"{verifyCertParam}={verifyCert}; ");

		return sb.ToString();
	}

	private void Validate()
	{
		if (serverCerts.Count > 0 && addresses.Count > serverCerts.Count)
			throw new InvalidConnectionStringException("The number of server certificates does not match the number of addresses." +
			" Each address must have its corresponding certificate.");

		if(!addresses.All(a=>a.Contains(':')))
		{
			throw new InvalidConnectionStringException("Address must be in format hostname:port");
		}
	}

	private void AppendAddresses(StringBuilder sb)
	{
		for (int i = 0; i < addresses.Count; i++)
		{
			sb.AppendFormat($"{addressParam}={addresses[i]}; ");
		}
	}

	private void AppendCerts(StringBuilder sb)
	{
		for (int i = 0; i < serverCerts.Count; i++)
		{
			sb.AppendFormat($"{serverCertParam}={serverCerts[i]}; ");
		}
	}

	private void SetParamValue(string v)
	{
		string[] s = v.Split("=");
		if (s.Length != 2)
			throw new InvalidConnectionStringException($"Parameter {v} is missing =");

		string key = s[0].Trim().ToLower();
		if (key.Equals(addressParam, StringComparison.Ordinal))
		{
			addresses.Add(s[1].Trim());
		}
		else if (key.Equals(poolSizeParam, StringComparison.Ordinal))
		{
			if (!int.TryParse(s[1].Trim().ToLower(), out poolSize))
				throw ParseException(key, s[1].Trim(), "integer");
		}
		else if (key.Equals(buffPoolSizeParam, StringComparison.Ordinal))
		{
			if (!int.TryParse(s[1].Trim().ToLower(), out bufferPoolSize))
				throw ParseException(key, s[1].Trim(), "integer");
		}
		else if (key.Equals(openTimeoutParam, StringComparison.Ordinal))
		{
			if (!int.TryParse(s[1].Trim().ToLower(), out openTimeout))
				throw ParseException(key, s[1].Trim(), "integer");
		}
		else if (key.Equals(retryTimeoutParam, StringComparison.Ordinal))
		{
			if (!int.TryParse(s[1].Trim().ToLower(), out retryTimeout))
				throw ParseException(key, s[1].Trim(), "integer");
		}
		else if (key.Equals(serviceNameParam, StringComparison.Ordinal))
		{
			serviceName = s[1];
		}
		else if (key.Equals(useSSLParam, StringComparison.Ordinal))
		{
			if (!bool.TryParse(s[1].Trim().ToLower(), out useSSL))
				throw ParseException(key, s[1].Trim(), "boolean");
		}
		else if (key.Equals(caCertParam, StringComparison.Ordinal))
		{
			caCert = s[1];
		}
		else if (key.Equals(serverCertParam, StringComparison.Ordinal))
		{
			serverCerts.Add(s[1]);
		}
		else if (key.Equals(verifyCertParam, StringComparison.Ordinal))
		{
			if (!bool.TryParse(s[1].Trim().ToLower(), out verifyCert))
				throw ParseException(key, s[1].Trim(), "boolean");
		}
		else
		{
			throw new InvalidConnectionStringException($"Unknown key {key}.");
		}
	}

	private InvalidConnectionStringException ParseException(string key, string value, string type) =>
		new InvalidConnectionStringException($"Couldn't parse param {key} value {value} as {type}.");
}
