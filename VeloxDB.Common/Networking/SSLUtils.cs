using System.Security.Cryptography.X509Certificates;

namespace VeloxDB.Networking;

internal class SSLUtils
{
	public static X509Certificate2 LoadCertificate(string path, string password = "", bool onlyPublicKey = false)
	{
		if (onlyPublicKey || path.EndsWith(".pfx"))
		{
			return new X509Certificate2(path, password);
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
