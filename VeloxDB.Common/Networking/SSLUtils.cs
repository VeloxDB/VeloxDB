using System.Security.Cryptography.X509Certificates;

namespace VeloxDB.Networking;

internal class SSLUtils
{
	public static X509Certificate2 LoadCertificate(string path, string password = "", bool onlyPublicKey = false)
	{
		if(path.EndsWith(".pfx"))
		{
			return X509CertificateLoader.LoadPkcs12FromFile(path, password);
		}

		if (onlyPublicKey)
		{
			return X509CertificateLoader.LoadCertificateFromFile(path);
		}
		else
		{
			if (string.IsNullOrEmpty(password))
			{
				return X509Certificate2.CreateFromPemFile(path);
			}
			else
			{
				return X509Certificate2.CreateFromEncryptedPemFile(path, password);
			}
		}
	}
}
