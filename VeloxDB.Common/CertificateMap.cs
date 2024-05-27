using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;

namespace VeloxDB.Networking;

internal class CertificateMap
{
	Dictionary<string, X509Certificate2> map;

	public int Count => map.Count;

	public CertificateMap(X509Certificate2[] certs)
	{
		map = new Dictionary<string, X509Certificate2>();
		foreach(X509Certificate2 cert in certs)
		{
			foreach(string name in GetCertificateHostnames(cert))
			{
				map.Add(name, cert);
			}
		}
	}

	IEnumerable<string> GetCertificateHostnames(X509Certificate2 certificate)
    {
        HashSet<string> hostnames = new();

        var sanExtension = certificate.Extensions.OfType<X509SubjectAlternativeNameExtension>().FirstOrDefault();

        if(sanExtension != null)
        {
            hostnames.UnionWith(sanExtension.EnumerateDnsNames());
            foreach(var ip in sanExtension.EnumerateIPAddresses())
            {
                hostnames.Add(ip.ToString());
            }
        }

        if (!string.IsNullOrEmpty(certificate.GetNameInfo(X509NameType.SimpleName, false)))
        {
            hostnames.Add(certificate.GetNameInfo(X509NameType.SimpleName, false));
        }

        return hostnames;
    }

	public static CertificateMap Create(string[] serverCerts)
	{
		X509Certificate2[] loaded = new X509Certificate2[serverCerts.Length];

		for (var i = 0; i < serverCerts.Length; i++)
		{
			var cert = serverCerts[i];
			loaded[i] = SSLUtils.LoadCertificate(cert, onlyPublicKey: true);
		}

		return new CertificateMap(loaded);
	}

	public X509Certificate2 GetCert(string address)
	{
		map.TryGetValue(address, out var result);
		return result;
	}
}
