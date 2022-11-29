using System;
using System.Linq;
using System.Net;
using Velox.Common;

namespace Velox.Config;

internal sealed class Endpoint
{
	public string Address {get; set;}
	public ushort? Port {get; set;}

	public void Override(Endpoint newEndpoint)
	{
		if(newEndpoint.Address != null)
			Address = newEndpoint.Address;

		if(newEndpoint.Port != null)
			Port = newEndpoint.Port;
	}

	public IPEndPoint ToIPEndPoint()
	{
		Checker.AssertNotNull(Address, Port);

		IPAddress[] ipAddresses;
		if (IPAddress.TryParse(Address, out IPAddress ipAddress))
		{
			ipAddresses = new IPAddress[] { ipAddress };
		}
		else
		{
			ipAddresses = Dns.GetHostAddresses(Address).Where(Address => !Address.Equals(IPAddress.IPv6Loopback)).ToArray();
		}

		if(ipAddresses.Length == 0)
		{
			throw new InvalidOperationException($"Given host address {Address} resolves to 0 ip addresses.");
		}

		if(ipAddresses.Length > 1)
		{
			throw new InvalidOperationException($"Given host address {Address} resolves to {ipAddresses.Length} ip addresses. Only one is expected.");
		}

		return new IPEndPoint(ipAddresses[0], (int)Port);
	}

	public override string ToString()
	{
		Checker.AssertNotNull(Address, Port);
		return $"{Address}:{Port}";
	}

	public static Endpoint FromString(string s)
	{
		Endpoint result = new Endpoint();
		int index = s.IndexOf(":");

		if(index == -1)
		{
			result.Address = s;
		}
		else
		{
			result.Port = ushort.Parse(s.AsSpan(index+1));

			if(index > 0)
				result.Address = s[..index];
		}

		return result;
	}

	public static implicit operator Endpoint(string s) => Endpoint.FromString(s);
}

